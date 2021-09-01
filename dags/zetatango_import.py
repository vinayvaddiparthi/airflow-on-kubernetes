import datetime
import logging
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Any, cast, Union, Dict

import attr
import heroku3
import pandas as pd
import pendulum
import psycopg2
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from psycopg2._psycopg import connection
from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ
import pyarrow.csv as pv, pyarrow.parquet as pq
from pyarrow._csv import ParseOptions, ReadOptions
from pyarrow.lib import ArrowInvalid
from json import dumps as json_dumps
from pyporky.symmetric import SymmetricPorky
from json import loads as json_loads
from base64 import b64decode
import yaml
from rubymarshal.reader import loads as rubymarshal_loads

from sqlalchemy import (
    text,
    func,
    create_engine,
    column,
    literal_column,
    literal,
    and_,
    union_all,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import Select, ClauseElement

from helpers.suspend_aws_env import SuspendAwsEnvVar
from utils import random_identifier
from dbt_extras.dbt_operator import DbtOperator
from dbt_extras.dbt_action import DbtAction
from utils.failure_callbacks import slack_dag


@attr.s
class DecryptionSpec:
    schema: str = attr.ib()
    table: str = attr.ib()
    columns: List[str] = attr.ib()
    format: Optional[Union[List[Optional[str]], str]] = attr.ib(default=None)
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
                SnowflakeHook(snowflake_connection).get_sqlalchemy_engine(),
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
    if table in ("versions", "job_reports", "job_statuses"):
        return f"⏭️️ Skipping table {table}"
    logging.info(f"start syncing table: {table}")
    stage_guid = random_identifier()
    with snowflake_engine.begin() as tx, cast(
        psycopg2.extensions.cursor, source_raw_conn.cursor()
    ) as cursor, tempfile.TemporaryDirectory() as tempdir:

        tx.execute(
            f"create or replace temporary stage {destination_schema}.{stage_guid} "
            f"file_format=(type=parquet)"
        ).fetchall()

        csv_filepath = Path(tempdir, table).with_suffix(".csv")
        pq_filepath = Path(tempdir, table).with_suffix(".pq")

        with csv_filepath.open("w+b") as csv_filedesc:
            logging.info(f"copy {source_schema}.{table}")

            cursor.copy_expert(
                f"copy {source_schema}.{table} to stdout "
                f"with csv header delimiter ',' quote '\"'",
                csv_filedesc,
            )

        try:
            logging.info(f"read {csv_filepath} for {table}")
            table_ = pv.read_csv(
                f"{csv_filepath}",
                read_options=ReadOptions(block_size=8388608),
                parse_options=ParseOptions(newlines_in_values=True),
            )
            logging.info(f"read {csv_filepath} for {table}: Done")
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


def _json_converter(o: Any) -> Union[str, int, float, bool, List, Dict, None]:
    if isinstance(o, datetime.date):
        return str(o)

    return o


def decrypt_pii_columns(
    snowflake_connection: str,
    decryption_specs: List[DecryptionSpec],
    target_schema: str,
) -> None:
    yaml.add_constructor(
        "!ruby/object:BigDecimal",
        lambda loader, node: float(loader.construct_scalar(node).split(":")[1]),  # type: ignore
    )

    yaml.add_constructor(
        "!ruby/object:ActiveSupport::TimeWithZone",
        lambda loader, node: loader.construct_yaml_timestamp(node.value[0][1]),
    )

    def _postprocess_marshal(field: Any) -> Any:
        return rubymarshal_loads(field) if field else None

    def _postprocess_yaml(field: Any) -> Optional[str]:
        return json_dumps(yaml.load(field), default=str) if field else None  # nosec

    def _postprocess_passthrough(field: Any) -> Any:
        return field if field else None

    postprocessors = {
        "marshal": _postprocess_marshal,
        "yaml": _postprocess_yaml,
        None: _postprocess_passthrough,
    }

    def _postprocess(list_: List, format: Union[str, List[str]]) -> List:
        if isinstance(format, str):
            return [postprocessors[format](field) for field in list_]

        return [postprocessors[format_](field) for field, format_ in zip(list_, format)]

    def _decrypt(
        row: pd.Series, format: Optional[Union[str, List[str]]] = None
    ) -> List[Any]:
        list_: List[Optional[bytes]] = []
        for field in row[3:]:
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

        return row[0:3].tolist() + (_postprocess(list_, format) if format else list_)

    engine = SnowflakeHook(snowflake_connection).get_sqlalchemy_engine()
    for spec in decryption_specs:
        dst_stage = random_identifier()
        dst_table = f"{target_schema}.{spec.schema}${spec.table}"

        with engine.begin() as tx:
            tx.execute(
                f"create or replace temporary stage "
                f"{target_schema}.{dst_stage} "  # nosec
                f"file_format=(type=parquet)"  # nosec
            ).fetchall()

            stmt = Select(
                columns=[
                    literal_column(f"{spec.table}.$1:id::integer").label("id"),
                    literal_column(f"{spec.table}.$1:updated_at::datetime").label(
                        "updated_at"
                    ),
                    func.sha2(literal_column("$1")).label("_hash"),
                ]
                + [
                    func.base64_decode_string(
                        literal_column(f"{spec.table}.$1:encrypted_{col}")
                    ).label(col)
                    for col in spec.columns
                ],
                from_obj=text(f"{spec.schema}.{spec.table}"),
            )

            select_froms: List[Select] = [
                Select(
                    [literal_column("$1").label("fields")],
                    from_obj=text(f"@{target_schema}.{dst_stage}"),
                ),
                Select(
                    [literal_column("$1").label("fields")],
                    from_obj=text(dst_table),
                ),
            ]

            try:
                unknown_hashes_whereclause: ClauseElement = literal_column(
                    "_hash"
                ).notin_(
                    Select(
                        [literal_column("$1:_hash::varchar")], from_obj=text(dst_table)
                    )
                )

                whereclause: ClauseElement = (
                    and_(spec.whereclause, unknown_hashes_whereclause)
                    if spec.whereclause is not None
                    else unknown_hashes_whereclause
                )
                dfs = pd.read_sql(stmt.where(whereclause), con=tx, chunksize=500)
            except ProgrammingError:
                dfs = pd.read_sql(
                    stmt.where(spec.whereclause)
                    if spec.whereclause is not None
                    else stmt,
                    con=tx,
                    chunksize=500,
                )

                select_froms = select_froms[:1]  # don't union if table doesn't exist.

            with SuspendAwsEnvVar():
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
                            text(
                                f"put file://{tempfile_.name} @{target_schema}.{dst_stage}"
                            )
                        ).fetchall()
            stmt = Select(
                [literal_column("*")],
                from_obj=union_all(*select_froms),
            )

            tx.execute(
                f"create or replace transient table {dst_table} as {stmt} "  # nosec
                f"qualify row_number() "
                f"over (partition by fields:id::integer order by fields:updated_at::datetime desc) = 1"
            ).fetchall()

            logging.info(f"🔓 Successfully decrypted {spec}")


def create_dag() -> DAG:
    with DAG(
        dag_id="zetatango_import",
        start_date=pendulum.datetime(
            2020, 4, 1, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval="0 */2 * * *",
        default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
        catchup=False,
        on_failure_callback=slack_dag("slack_data_alerts"),
        max_active_runs=1,
    ) as dag:
        import_core_prod = PythonOperator(
            task_id="zt-production-elt-core__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-production-elt-core",
                "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_00749F2C263CE53C5_URL",
                "snowflake_connection": "snowflake_zetatango_production",
                "snowflake_schema": "CORE_PRODUCTION",
            },
            executor_config={
                "resources": {
                    "requests": {"memory": "4Gi"},
                },
            },
        )

        decrypt_core_prod = PythonOperator(
            task_id="zt-production-elt-core__pii_decryption",
            python_callable=decrypt_pii_columns,
            op_kwargs={
                "snowflake_connection": "snowflake_zetatango_production",
                "decryption_specs": [
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="MERCHANT_ATTRIBUTES",
                        columns=["value"],
                        whereclause=literal_column("$1:key").in_(
                            [
                                "industry",
                                "bank_connection_required",
                                "selected_bank_account",
                                "manual_sic_code",
                                "manual_business_online",
                                "selected_sales_volume_accounts",
                                "merchant_black_flag",
                                "merchant_black_flag_reason",
                                "merchant_black_flag_date",
                                "merchant_red_flag",
                                "merchant_red_flag_reason",
                                "merchant_red_flag_date",
                                "merchant_on_hold_flag",
                                "merchant_on_hold_flag_reason",
                                "merchant_on_hold_flag_date",
                            ]
                        ),
                    ),
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="LENDING_ADJUDICATIONS",
                        columns=[
                            "offer_results",
                            "adjudication_results",
                            "notes",
                        ],
                        format=["yaml", "yaml", None],
                    ),
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="LENDING_ADJUDICATION_DECISIONS",
                        columns=["notes"],
                    ),
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="LENDING_LOAN_ATTRIBUTES",
                        columns=["value"],
                        whereclause=literal_column("$1:key").in_(["external_id"]),
                    ),
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="QUICKBOOKS_ACCOUNTING_TRANSACTIONS",
                        columns=["account", "split"],
                    ),
                    DecryptionSpec(
                        schema="CORE_PRODUCTION",
                        table="LEADS",
                        columns=[
                            "applicant_email",
                            "applicant_first_name",
                            "applicant_last_name",
                            "merchant_name",
                        ],
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
                "resources": {
                    "requests": {"memory": "2Gi"},
                },
            },
        )

        import_idp_prod = PythonOperator(
            task_id="zt-production-elt-idp__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-production-elt-idp",
                "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_0DB594617CE5BEC42_URL",
                "snowflake_connection": "snowflake_zetatango_production",
                "snowflake_schema": "IDP_PRODUCTION",
            },
            executor_config={
                "resources": {
                    "requests": {"memory": "2Gi"},
                },
            },
        )

        decrypt_idp_prod = PythonOperator(
            task_id="zt-production-elt-idp__pii_decryption",
            python_callable=decrypt_pii_columns,
            op_kwargs={
                "snowflake_connection": "snowflake_zetatango_production",
                "decryption_specs": [
                    DecryptionSpec(
                        schema="IDP_PRODUCTION",
                        table="POLY_PROPERTIES",
                        columns=["value"],
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
                "resources": {
                    "requests": {"memory": "2Gi"},
                },
            },
        )

        import_kyc_prod = PythonOperator(
            task_id="zt-production-elt-kyc__import",
            python_callable=export_to_snowflake,
            op_kwargs={
                "heroku_app": "zt-production-elt-kyc",
                "heroku_endpoint_url_env_var": "DATABASE_ENDPOINT_0467EC30D24A2723A_URL",
                "snowflake_connection": "snowflake_zetatango_production",
                "snowflake_schema": "KYC_PRODUCTION",
            },
            executor_config={
                "resources": {
                    "requests": {"memory": "2Gi"},
                },
            },
        )

        decrypt_kyc_prod = PythonOperator(
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
                            [
                                "default_beacon_score",
                                "sin",
                                "suffix",
                                "file_number",
                            ]
                        ),
                    ),
                    DecryptionSpec(
                        schema="KYC_PRODUCTION",
                        table="ENTITIES_BANK_ACCOUNT_ATTRIBUTES",
                        columns=["value"],
                        whereclause=literal_column("$1:key").in_(
                            [
                                "transit_number",
                                "institution_number",
                                "account_number",
                                "flinks_account_type",
                                "flinks_account_id",
                                "stale",
                            ]
                        ),
                    ),
                    DecryptionSpec(
                        schema="KYC_PRODUCTION",
                        table="ENTITIES_BUSINESS_ATTRIBUTES",
                        columns=["value"],
                        whereclause=literal_column("$1:key").in_(
                            [
                                "file_number",
                            ]
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
                "resources": {
                    "requests": {"memory": "2Gi"},
                },
            },
        )

        dbt_run = DbtOperator(
            task_id="dbt_run",
            execution_timeout=timedelta(hours=1),
            action=DbtAction.run,
        )

        dbt_snapshot = DbtOperator(
            task_id="dbt_snapshot",
            execution_timeout=timedelta(hours=1),
            action=DbtAction.snapshot,
        )

        dbt_test = DbtOperator(
            task_id="dbt_test",
            execution_timeout=timedelta(hours=1),
            action=DbtAction.test,
        )

        dag << import_core_prod >> decrypt_core_prod
        dag << import_idp_prod >> decrypt_idp_prod
        dag << import_kyc_prod >> decrypt_kyc_prod
        (
            [
                decrypt_core_prod,
                decrypt_kyc_prod,
                decrypt_idp_prod,
            ]
            >> dbt_run
            >> dbt_snapshot
            >> dbt_test
        )

    return dag


if __name__ == "__main__":
    import os
    from unittest.mock import patch
    from snowflake.sqlalchemy import URL

    with patch(
        "dags.zetatango_import.SnowflakeHook.get_sqlalchemy_engine",
        return_value=create_engine(
            URL(
                account="thinkingcapital.ca-central-1.aws",
                user=os.environ["SNOWFLAKE_USERNAME"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                database="ZETATANGO",
                warehouse="ETL",
            )
        ),
    ):
        decrypt_pii_columns(
            "abc",
            [
                DecryptionSpec(
                    schema="KYC_STAGING",
                    table="INDIVIDUAL_ATTRIBUTES",
                    columns=["value"],
                    format="marshal",
                    whereclause=literal_column("$1:key").in_(["default_beacon_score"]),
                )
            ],
            target_schema="PII_STAGING",
        )

else:
    globals()["ztimportdag"] = create_dag()
