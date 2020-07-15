import logging
import os
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, cast, Any

import attr
import heroku3
import pandas as pd
import pendulum
import psycopg2
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from psycopg2._psycopg import connection
from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ
import pyarrow.csv as pv, pyarrow.parquet as pq
from pyarrow._csv import ParseOptions, ReadOptions
from pyarrow.lib import ArrowInvalid

from sqlalchemy import text, func, create_engine, column, literal_column, literal, and_
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select, ClauseElement

from utils import random_identifier


@attr.s
class DecryptionSpec:
    schema: str = attr.ib()
    table: str = attr.ib()
    columns: List[str] = attr.ib()
    format: Optional[str] = attr.ib(default=None)
    catalog: str = attr.ib(default=None)
    whereclause: Optional[ClauseElement] = attr.ib(default=None)


def export_to_snowflake(
    snowflake_connection: str,
    snowflake_schema: str,
    source_schema: str = "public",
    heroku_app: Optional[str] = None,
    heroku_endpoint_url_env_var: str = "DATABASE_URL",
    heroku_postgres_connection: Optional[str] = None,
) -> None:
    if not (heroku_postgres_connection or heroku_app):
        raise Exception(
            "Must receive either `heroku_postgres_connection` or `heroku_app` as argument."
        )

    source_engine = (
        PostgresHook(heroku_postgres_connection).get_sqlalchemy_engine(
            engine_kwargs={"isolation_level": "REPEATABLE_READ"}
        )
        if heroku_postgres_connection
        else create_engine(
            heroku3.from_key(
                HttpHook.get_connection("heroku_production_api_key").password
            )
            .app(heroku_app)
            .config()[heroku_endpoint_url_env_var],
            isolation_level="REPEATABLE_READ",
        )
    )

    snowflake_engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()

    with source_engine.begin() as tx:
        tables = (
            x[0]
            for x in tx.execute(
                Select(
                    columns=[column("table_name")],
                    from_obj=text('"information_schema"."tables"'),
                    whereclause=and_(
                        literal_column("table_schema") == literal(source_schema),
                        literal_column("table_type") == literal("BASE TABLE"),
                    ),
                )
            ).fetchall()
        )

    source_raw_conn: connection = source_engine.raw_connection()
    try:
        source_raw_conn.set_isolation_level(ISOLATION_LEVEL_REPEATABLE_READ)
        output = [
            stage_table_in_snowflake(
                source_raw_conn,
                snowflake_engine,
                source_schema,
                snowflake_schema,
                table,
            )
            for table in tables
        ]
        print(*output, sep="\n")
    finally:
        source_raw_conn.close()


def stage_table_in_snowflake(
    source_raw_conn: connection,
    snowflake_engine: Engine,
    source_schema: str,
    destination_schema: str,
    table: str,
) -> str:
    stage_guid = random_identifier()
    with snowflake_engine.begin() as tx:
        tx.execute(
            f"create or replace temporary stage {destination_schema}.{stage_guid} "
            f"file_format=(type=parquet)"
        ).fetchall()

        with cast(
            psycopg2.extensions.cursor, source_raw_conn.cursor()
        ) as cursor, tempfile.TemporaryDirectory() as tempdir:
            csv_filepath = Path(tempdir, table).with_suffix(".csv")
            pq_filepath = Path(tempdir, table).with_suffix(".pq")

            with csv_filepath.open("w+b") as csv_filedesc:
                cursor.copy_expert(
                    f"copy {source_schema}.{table} to stdout "
                    f"with csv header delimiter ',' quote '\"'",
                    csv_filedesc,
                )

            try:
                table_ = pv.read_csv(
                    f"{csv_filepath}",
                    read_options=ReadOptions(block_size=8388608),
                    parse_options=ParseOptions(newlines_in_values=True),
                )
            except ArrowInvalid as exc:
                return f"❌ Failed to read table {table}: {exc}"

            if table_.num_rows == 0:
                return f"📝️ Skipping empty table {table}"

            pq.write_table(table_, f"{pq_filepath}")

            tx.execute(
                f"put file://{pq_filepath} @{destination_schema}.{stage_guid}"
            ).fetchall()

        tx.execute(
            f"create or replace transient table {destination_schema}.{table} as "  # nosec
            f"select $1 as fields from @{destination_schema}.{stage_guid}"  # nosec
        ).fetchall()

    return f"✔️ Successfully loaded table {table}"


def decrypt_pii_columns(
    snowflake_connection: str,
    decryption_specs: List[DecryptionSpec],
    target_schema: str,
) -> None:
    from pyporky.symmetric import SymmetricPorky
    from json import loads as json_loads, dumps as json_dumps
    from base64 import b64decode
    import yaml
    from rubymarshal.reader import loads as rubymarshal_loads

    yaml.add_constructor(
        "!ruby/object:BigDecimal",
        lambda loader, node: float(loader.construct_scalar(node).split(":")[1]),
    )

    def _postprocess_marshal(list_: List) -> List[Any]:
        return [(rubymarshal_loads(field) if field else None) for field in list_]

    def _postprocess_yaml(list_: List) -> List[str]:
        return [
            json_dumps(yaml.load(field) if field else None) for field in list_  # nosec
        ]

    postprocessors = {"marshal": _postprocess_marshal, "yaml": _postprocess_yaml}

    try:
        del os.environ["AWS_ACCESS_KEY_ID"]
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    except KeyError:
        pass

    def _decrypt(row: pd.Series, format: Optional[str] = None) -> List[Any]:
        list_: List[Optional[bytes]] = []
        for field in row[1:]:
            if not field:
                list_.append(None)
                continue

            crypto_material = json_loads(field)
            list_.append(
                SymmetricPorky(aws_region="ca-central-1").decrypt(
                    enciphered_dek=b64decode(crypto_material["key"]),
                    enciphered_data=b64decode(crypto_material["data"]),
                    nonce=b64decode(crypto_material["nonce"]),
                )
            )

        return [row[0]] + (postprocessors[format](list_) if format else list_)

    engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
    for spec in decryption_specs:
        dst_stage = random_identifier()
        stmt = Select(
            columns=[literal_column(f"{spec.table}.$1:id").label("id")]
            + [
                func.base64_decode_string(
                    literal_column(f"{spec.table}.$1:encrypted_{col}")
                ).label(col)
                for col in spec.columns
            ],
            from_obj=text(f"{spec.schema}.{spec.table}"),
            whereclause=spec.whereclause,
        )

        with engine.begin() as tx:
            tx.execute(
                f"CREATE OR REPLACE TEMPORARY STAGE {target_schema}.{dst_stage} "  # nosec
                f"FILE_FORMAT=(TYPE=PARQUET)"  # nosec
            ).fetchall()

            dfs = pd.read_sql(stmt, con=tx, chunksize=500)
            for df in dfs:
                with tempfile.NamedTemporaryFile() as tempfile_:
                    df = df.apply(
                        axis=1,
                        func=_decrypt,
                        result_type="broadcast",
                        args=(spec.format,),
                    )
                    df.to_parquet(
                        tempfile_.name, engine="fastparquet", compression="gzip"
                    )
                    tx.execute(
                        f"PUT file://{tempfile_.name} @{target_schema}.{dst_stage}"  # nosec
                    ).fetchall()

            tx.execute(
                f"CREATE OR REPLACE TRANSIENT TABLE {target_schema}.{spec.schema}${spec.table} AS "  # nosec
                f"SELECT $1 AS FIELDS FROM @{target_schema}.{dst_stage}"  # nosec
            ).fetchall()

            logging.info(f"🔓 Successfully decrypted {spec}")


def create_dag() -> DAG:
    with DAG(
        dag_id="zetatango_import",
        start_date=pendulum.datetime(
            2020, 4, 1, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="30 0,9-21/4 * * *",
        default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    ) as dag:
        dag << PythonOperator(
            task_id="zt-production-elt-core__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-production-elt-core",
                "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_00749F2C263CE53C5_URL",
                "snowflake_connection": "snowflake_zetatango_production",
                "snowflake_schema": "CORE_PRODUCTION",
            },
        ) >> PythonOperator(
            task_id="zt-production-elt-core__pii_decryption",
            python_callable=decrypt_pii_columns,
            op_kwargs={
                "snowflake_connection": "snowflake_zetatango_production",
                "decryption_specs": [
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="MERCHANT_ATTRIBUTES",
                        columns=["value"],
                        whereclause=literal_column("$1:key").in_(["industry"]),
                    ),
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="LENDING_ADJUDICATIONS",
                        columns=["offer_results"],
                        format="yaml",
                    ),
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="LENDING_LOAN_ATTRIBUTES",
                        columns=["value"],
                        whereclause=literal_column("$1:key").in_(["external_id"]),
                    ),
                ],
                "target_schema": "PII_PRODUCTION",
            },
            executor_config={
                "KubernetesExecutor": {
                    "annotations": {
                        "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                        "KubernetesAirflowProductionZetatangoPiiRole"
                    }
                },
                "resources": {"request_memory": "2Gi", "limit_memory": "2Gi"},
            },
        )

        dag << PythonOperator(
            task_id="zt-production-elt-idp__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-production-elt-idp",
                "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_0DB594617CE5BEC42_URL",
                "snowflake_connection": "snowflake_zetatango_production",
                "snowflake_schema": "IDP_PRODUCTION",
            },
        )

        dag << PythonOperator(
            task_id="zt-production-elt-kyc__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-production-elt-kyc",
                "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_0467EC30D24A2723A_URL",
                "snowflake_connection": "snowflake_zetatango_production",
                "snowflake_schema": "KYC_PRODUCTION",
            },
        ) >> PythonOperator(
            task_id="zt-production-elt-kyc__pii_decryption",
            python_callable=decrypt_pii_columns,
            op_kwargs={
                "snowflake_connection": "snowflake_zetatango_production",
                "decryption_specs": [
                    DecryptionSpec(
                        schema="KYC_PRODUCTION",
                        table="INDIVIDUALS_APPLICANTS",
                        columns=[
                            "date_of_birth",
                            "first_name",
                            "last_name",
                            "middle_name",
                        ],
                    ),
                    DecryptionSpec(
                        schema="KYC_PRODUCTION",
                        table="INDIVIDUAL_ATTRIBUTES",
                        columns=["value"],
                        format="marshal",
                        whereclause=literal_column("$1:key").in_(
                            ["default_beacon_score"]
                        ),
                    ),
                ],
                "target_schema": "PII_PRODUCTION",
            },
            executor_config={
                "KubernetesExecutor": {
                    "annotations": {
                        "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                        "KubernetesAirflowProductionZetatangoPiiRole"
                    }
                },
                "resources": {"request_memory": "2Gi", "limit_memory": "2Gi"},
            },
        )

        dag << PythonOperator(
            task_id="zt-staging-elt-core__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-staging-elt-core",
                "snowflake_connection": "snowflake_zetatango_staging",
                "snowflake_schema": "CORE_STAGING",
            },
        ) >> PythonOperator(
            task_id="zt-staging-elt-core__pii_decryption",
            python_callable=decrypt_pii_columns,
            op_kwargs={
                "snowflake_connection": "snowflake_zetatango_staging",
                "decryption_specs": [
                    DecryptionSpec(
                        schema="CORE_STAGING",
                        table="MERCHANT_ATTRIBUTES",
                        columns=["value"],
                        whereclause=literal_column("$1:key").in_(["industry"]),
                    ),
                    DecryptionSpec(
                        schema="CORE_STAGING",
                        table="LENDING_ADJUDICATIONS",
                        columns=["offer_results"],
                        format="yaml",
                    ),
                    DecryptionSpec(
                        schema="CORE_STAGING",
                        table="LENDING_LOAN_ATTRIBUTES",
                        columns=["value"],
                        whereclause=literal_column("$1:key").in_(["external_id"]),
                    ),
                ],
                "target_schema": "PII_STAGING",
            },
            executor_config={
                "KubernetesExecutor": {
                    "annotations": {
                        "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                        "KubernetesAirflowNonProdZetatangoPiiRole"
                    },
                    "resources": {"request_memory": "2Gi", "limit_memory": "2Gi"},
                }
            },
        )

        dag << PythonOperator(
            task_id="zt-staging-elt-idp__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-staging-elt-idp",
                "snowflake_connection": "snowflake_zetatango_staging",
                "snowflake_schema": "IDP_STAGING",
            },
        )

        dag << PythonOperator(
            task_id="zt-staging-elt-kyc__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-staging-elt-kyc",
                "snowflake_connection": "snowflake_zetatango_staging",
                "snowflake_schema": "KYC_STAGING",
            },
        ) >> PythonOperator(
            task_id="zt-staging-elt-kyc__pii_decryption",
            python_callable=decrypt_pii_columns,
            op_kwargs={
                "snowflake_connection": "snowflake_zetatango_staging",
                "decryption_specs": [
                    DecryptionSpec(
                        schema="KYC_STAGING",
                        table="INDIVIDUAL_ATTRIBUTES",
                        columns=["value"],
                        format="marshal",
                        whereclause=literal_column("$1:key").in_(
                            ["default_beacon_score"]
                        ),
                    )
                ],
                "target_schema": "PII_STAGING",
            },
            executor_config={
                "KubernetesExecutor": {
                    "annotations": {
                        "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                        "KubernetesAirflowNonProdZetatangoPiiRole"
                    },
                    "resources": {"request_memory": "2Gi", "limit_memory": "2Gi"},
                }
            },
        )

        dag << PythonOperator(
            task_id="dbt_snapshot",
            pool="snowflake_pool",
            execution_timeout=datetime.timedelta(hours=1),
            action=DbtAction.snapshot,
        )

    return dag


if __name__ == "__main__":
    pass
else:
    globals()["ztimportdag"] = create_dag()
