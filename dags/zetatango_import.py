import logging
import os
import random
import string
import tempfile
from pathlib import Path
from typing import List, Optional

import attr
import heroku3
import pandas as pd
import pendulum
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow import DAG

from sqlalchemy import text, func, create_engine, column, literal_column, literal
from sqlalchemy.engine import Transaction, Engine
from sqlalchemy.sql import Select, ClauseElement


def __random():
    return "".join(random.choice(string.ascii_uppercase) for _ in range(24))  # nosec


@attr.s
class DecryptionSpec:
    schema: str = attr.ib()
    table: str = attr.ib()
    columns: List[str] = attr.ib()
    format: Optional[str] = attr.ib(default=False)
    catalog: str = attr.ib(default=None)
    whereclause: Optional[ClauseElement] = attr.ib(default=None)


def export_to_snowflake(
    snowflake_connection: str,
    snowflake_schema: str,
    source_schema: str = "public",
    heroku_app: str = None,
    heroku_endpoint_url_env_var: str = "DATABASE_URL",
    heroku_postgres_connection: str = None,
):
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
                    whereclause=literal_column("table_schema")
                    == literal(source_schema),
                )
            ).fetchall()
        )

        output = [
            stage_table_in_snowflake(
                tx, snowflake_engine, source_schema, snowflake_schema, table
            )
            for table in tables
        ]

    print(*output, sep="\n")


def stage_table_in_snowflake(
    source_tx: Transaction,
    snowflake_engine: Engine,
    source_schema: str,
    destination_schema: str,
    table: str,
):
    with tempfile.TemporaryDirectory() as temp_dir_path:
        stage_guid = __random()
        with snowflake_engine.begin() as tx:
            tx.execute(
                f"CREATE OR REPLACE TEMPORARY STAGE {destination_schema}.{stage_guid} "
                f"FILE_FORMAT=(TYPE=PARQUET)"
            ).fetchall()

            try:
                for df in pd.read_sql_table(
                    table, source_tx, source_schema, chunksize=10000
                ):
                    file_guid = __random()
                    path = Path(temp_dir_path) / file_guid

                    df.to_parquet(f"{path}", engine="fastparquet", compression="gzip")
                    tx.execute(
                        f"PUT file://{path} @{destination_schema}.{stage_guid}"
                    ).fetchall()
            except ValueError as ve:
                return f"⚠️ Caught ValueError reading table {table}: {ve}"

            tx.execute(
                f"CREATE OR REPLACE TRANSIENT TABLE {destination_schema}.{table} AS "
                f"SELECT * FROM @{destination_schema}.{stage_guid}"
            ).fetchall()

    return f"✔️ Successfully loaded table {table}"


def decrypt_pii_columns(
    snowflake_connection: str,
    decryption_specs: List[DecryptionSpec],
    target_schema: str,
):
    from pyporky.symmetric import SymmetricPorky
    from json import loads as json_loads, dumps as json_dumps
    from base64 import b64decode

    def __postprocess_marshal(list_: List):
        from rubymarshal.reader import loads as rubymarshal_loads

        return [rubymarshal_loads(field) for field in list_]

    def __postprocess_yaml(list_: List):
        import yaml

        yaml.add_constructor(
            "!ruby/object:BigDecimal",
            lambda loader, node: float(loader.construct_scalar(node).split(":")[1]),
        )

        return [json_dumps(yaml.load(field)) for field in list_]  # nosec

    postprocessors = {"marshal": __postprocess_marshal, "yaml": __postprocess_yaml}

    try:
        del os.environ["AWS_ACCESS_KEY_ID"]
        del os.environ["AWS_SECRET_ACCESS_KEY"]
    except KeyError:
        pass

    def __decrypt(row, format=None):
        list_ = []
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

    for spec in decryption_specs:
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

        engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
        with engine.begin() as tx, tempfile.TemporaryDirectory() as output_directory:
            path = Path(output_directory) / f"{spec.table}.parquet"

            df = pd.read_sql(stmt, con=tx)
            df = df.apply(
                axis=1, func=__decrypt, result_type="broadcast", args=(spec.format,)
            )
            df.to_parquet(path, engine="fastparquet", compression="gzip")

            stage = __random()

            logging.debug(
                [
                    tx.execute(stmt).fetchall()
                    for stmt in [
                        f"CREATE OR REPLACE TEMPORARY STAGE {target_schema}.{stage} FILE_FORMAT=(TYPE=PARQUET)",
                        f"PUT file://{path} @{target_schema}.{stage}",
                        f"CREATE OR REPLACE TRANSIENT TABLE {target_schema}.{spec.schema}${spec.table} AS SELECT * FROM @{target_schema}.{stage}",
                    ]
                ]
            )

        logging.info(f"🔓 Successfully decrypted {spec}")


with DAG(
    dag_id="zetatango_import",
    start_date=pendulum.datetime(
        2020, 4, 1, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="0 0,8-20 * * 1-5",
) as dag:
    dag << PythonOperator(
        task_id="zt-production-elt-core__import",
        python_callable=export_to_snowflake,
        op_kwargs={
            "heroku_app": "zt-production-elt-core",
            "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_06926445112852C5F_URL",
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
            ],
            "target_schema": "PII_PRODUCTION",
        },
        executor_config={
            "KubernetesExecutor": {
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                    "KubernetesAirflowProductionZetatangoPiiRole"
                }
            }
        },
    )

    dag << PythonOperator(
        task_id="zt-production-elt-idp__import",
        python_callable=export_to_snowflake,
        op_kwargs={
            "heroku_app": "zt-production-elt-idp",
            "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_06730E622159E3664_URL",
            "snowflake_connection": "snowflake_zetatango_production",
            "snowflake_schema": "IDP_PRODUCTION",
        },
    )

    dag << PythonOperator(
        task_id="zt-production-elt-kyc__import",
        python_callable=export_to_snowflake,
        op_kwargs={
            "heroku_app": "zt-production-elt-kyc",
            "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_075C067EF5D19D7F2_URL",
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
                    table="INDIVIDUAL_ATTRIBUTES",
                    columns=["value"],
                    format="marshal",
                    whereclause=literal_column("$1:key").in_(["default_beacon_score"]),
                )
            ],
            "target_schema": "PII_PRODUCTION",
        },
        executor_config={
            "KubernetesExecutor": {
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                    "KubernetesAirflowProductionZetatangoPiiRole"
                }
            }
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
            ],
            "target_schema": "PII_STAGING",
        },
        executor_config={
            "KubernetesExecutor": {
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                    "KubernetesAirflowNonProdZetatangoPiiRole"
                }
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
                    whereclause=literal_column("$1:key").in_(["default_beacon_score"]),
                )
            ],
            "target_schema": "PII_STAGING",
        },
        executor_config={
            "KubernetesExecutor": {
                "annotations": {
                    "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
                    "KubernetesAirflowNonProdZetatangoPiiRole"
                }
            }
        },
    )
