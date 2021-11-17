import logging
import json
import pendulum
from datetime import timedelta
from sqlalchemy import Table, MetaData, VARCHAR
from sqlalchemy.sql import select, func, cast
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
    "max_active_runs": 1,
    "tags": ["business reports"],
    "description": "A workflow to import business reports from s3 to snowflake",
    "snowflake_conn_id": "snowflake_production",
    "schema": "KYC_PRODUCTION",
    "database": "ZETATANGO",
    "s3_conn_id": "s3_dataops",
    "s3_bucket": "ztportal-upload-production",
}


def get_file_keys(
    snowflake_conn_id: str,
    schema: str,
) -> list:
    engine = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_sqlalchemy_engine()
    metadata_obj = MetaData()
    entities_business_reports = Table(
        "entities_business_reports",
        metadata_obj,
        autoload_with=engine,
        schema=schema,
    )
    raw_business_report_responses = Table(
        "raw_business_report_responses_test",
        metadata_obj,
        autoload_with=engine,
        schema=schema,
    )
    stmt = (
        select(
            columns=[
                cast(
                    func.get(entities_business_reports.c.fields, "lookup_key"), VARCHAR
                ).label("lookup_key")
            ]
        )
        .where(
            cast(
                func.get(entities_business_reports.c.fields, "report_type"), VARCHAR
            ).in_(["equifax_business_default", "equifax_personal_default"])
        )
        .where(
            cast(
                func.get(entities_business_reports.c.fields, "lookup_key"), VARCHAR
            ).notin_(select(columns=[raw_business_report_responses.c.lookup_key]))
        )
    )
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(stmt)]


def download_business_report(
    file_number: int,
    file_key: str,
    s3_conn_id: str,
    s3_bucket: str,
    snowflake_conn_id: str,
    schema: str,
    ts: str,
    **_: None,
) -> None:
    logging.info(
        f"(📂 #{file_number}) Processing business_report located in key={file_key}, bucket={s3_bucket}..."
    )
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_object = s3_hook.get_key(key=file_key, bucket_name=s3_bucket)
    body = json.loads(s3_object.get()["Body"].read())
    body_decrypted = str(
        SymmetricPorky(aws_region="ca-central-1").decrypt(
            enciphered_dek=b64decode(body["key"], b"-_"),
            enciphered_data=b64decode(body["data"], b"-_"),
            nonce=b64decode(body["nonce"], b"-_"),
        ),
        "utf-8",
    )
    logging.info(
        f"🔑 (#{file_number}) Decrypted business report located in key={file_key}, bucket={s3_bucket}..."
    )
    engine = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_sqlalchemy_engine()
    metadata_obj = MetaData()
    raw_business_report_responses = Table(
        "raw_business_report_responses_test",
        metadata_obj,
        autoload_with=engine,
        schema=schema,
    )
    with engine.begin() as tx:
        tx.execute(
            raw_business_report_responses.insert(),
            {
                "lookup_key": file_key,
                "raw_response": body_decrypted,
                "last_modified_at": s3_object.get()["LastModified"],
                "batch_import_timestamp": ts,
            },
        )
        logging.info(
            f"✨️️ (#{file_number}) Successfully stored business report located in key={file_key}, bucket={s3_bucket}"
            f"in {raw_business_report_responses} table..."
        )


def download_business_report_worker(file_number: int, file_key: str) -> PythonOperator:
    return PythonOperator(
        task_id=f"download_business_report_{file_number}",
        python_callable=download_business_report,
        op_kwargs={
            **default_args,
            **{"file_number": file_number, "file_key": file_key},
        },
        pool="business_reports_pool",
        provide_context=True,
    )


with DAG(
    dag_id="business_reports_import_test",
    schedule_interval="0 */2 * * *",
    default_args=default_args,
    template_searchpath="dags/sql",
) as dag:
    create_target_table = SnowflakeOperator(
        task_id="create_target_table",
        sql="business_reports/create_table.sql",
        params={"table_name": "raw_business_report_responses_test"},
    )
    for i, s3_file_key in enumerate(
        get_file_keys(snowflake_conn_id="snowflake_production", schema="KYC_PRODUCTION")
    ):
        create_target_table >> download_business_report_worker(i + 1, s3_file_key)
