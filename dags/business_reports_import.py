"""
#### Description
This workflow imports various business reports stored in the ztportal-upload-production s3 bucket.
"""
import time
import logging
import json
import pendulum
import pandas as pd

from datetime import timedelta
from sqlalchemy import Table, MetaData, VARCHAR, text
from sqlalchemy.sql import select, func, text, literal_column, literal, join
from base64 import b64decode

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from pyporky.symmetric import SymmetricPorky
from utils.failure_callbacks import slack_dag

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_dag("slack_data_alerts"),
    "start_date": pendulum.datetime(
        2021, 11, 12, tzinfo=pendulum.timezone("America/Toronto")
    ),
    "catchup": False,
    "tags": ["business reports"],
    "description": "A workflow to import business reports from s3 to snowflake",
}

dag = DAG(
    dag_id="business_reports_import",
    schedule_interval="0 */2 * * *",
    default_args=default_args,
    template_searchpath="dags/sql",
)
dag.doc_md = __doc__

SNOWFLAKE_CONN = "snowflake_production"
SCHEMA = "KYC_PRODUCTION"


def _download_business_reports(
    snowflake_conn_id: str,
    schema: str,
    s3_conn_id: str,
    s3_bucket: str,
    ts: str,
    **_: None,
) -> None:
    start_time = time.time()
    engine = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_sqlalchemy_engine()
    metadata = MetaData()

    stmt = text(
        """
        select lookup_key
        from ANALYTICS_PRODUCTION.DBT_ARIO.DIM_BUSINESS_REPORTS
        where report_type in ('equifax_business_default', 'equifax_personal_default')
            and lookup_key not in (
                select lookup_key
                from ZETATANGO.KYC_PRODUCTION.RAW_BUSINESS_REPORT_RESPONSES
            )
        """
    )

    df_reports_to_download = pd.read_sql_query(
        sql=stmt,
        con=engine,
    )

    raw_business_report_responses = Table(
        "raw_business_report_responses",
        metadata,
        autoload_with=engine,
        schema=schema,
    )

    logging.info(f"📂 Processing {df_reports_to_download.size} business_reports...")

    s3 = S3Hook(aws_conn_id=s3_conn_id)
    for i, row in df_reports_to_download.iterrows():
        logging.info(f"Lookup_key -> {row[0]}")
        s3_object = s3.get_key(key=row[0], bucket_name=s3_bucket)
        body = json.loads(s3_object.get()["Body"].read())

        logging.info(f"Metadata -> {s3_object.get()['Metadata']}")

        body_decrypted = str(
            SymmetricPorky(aws_region="ca-central-1").decrypt(
                enciphered_dek=b64decode(body["key"], b"-_"),
                enciphered_data=b64decode(body["data"], b"-_"),
                nonce=b64decode(body["nonce"], b"-_"),
            ),
            "utf-8",
        )

        logging.info(
            f"Decrypted business report in {row[0]}, bucket={s3_bucket} ({i + 1})"
        )

        with engine.begin() as tx:
            batch_import_timestamp = ts
            select_query = select(
                columns=[
                    literal_column(f"'{row[0]}'").label("lookup_key"),
                    func.parse_json(body_decrypted).label("raw_response"),
                    literal_column(f"'{s3_object.get()['LastModified']}'").label(
                        "last_modified_at"
                    ),
                    literal_column(f"'{batch_import_timestamp}'").label(
                        "batch_import_timestamp"
                    ),
                ]
            )

            insert_query = raw_business_report_responses.insert().from_select(
                [
                    "lookup_key",
                    "raw_response",
                    "last_modified_at",
                    "batch_import_timestamp",
                ],
                select_query,
            )

            tx.execute(insert_query)

            logging.info(f"✔️ Successfully inserted {row[0]} to Snowflake.")
    duration = time.time() - start_time
    logging.info(
        f"Downloaded {df_reports_to_download.size} business reports in {duration} seconds"
    )


create_target_table = SnowflakeOperator(
    task_id="create_target_table",
    sql=f"business_reports/create_table.sql",
    params={"table_name": "raw_business_report_responses"},
    schema=SCHEMA,
    database="ZETATANGO",
    snowflake_conn_id=SNOWFLAKE_CONN,
    dag=dag,
)

download_business_reports = PythonOperator(
    task_id="download_business_reports",
    python_callable=_download_business_reports,
    provide_context=True,
    op_kwargs={
        "snowflake_conn_id": SNOWFLAKE_CONN,
        "schema": SCHEMA,
        "s3_conn_id": "s3_dataops",
        "s3_bucket": "ztportal-upload-production",
    },
    dag=dag,
)

create_target_table >> download_business_reports
