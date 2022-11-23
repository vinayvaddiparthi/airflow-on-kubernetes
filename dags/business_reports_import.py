"""
#### Description
This workflow imports various business reports stored in the ztportal-upload-production s3 bucket.
"""
import time
import logging
import json

import pendulum
from datetime import timedelta

import sqlalchemy.engine
from sqlalchemy import Table, MetaData, VARCHAR
from sqlalchemy.sql import select, func, cast
from concurrent.futures.thread import ThreadPoolExecutor
from base64 import b64decode
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pyporky.symmetric import SymmetricPorky
from utils.failure_callbacks import slack_task
from custom_operators.rbac_python_operator import RBACPythonOperator
from functools import partial

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_task("slack_data_alerts"),
    "start_date": pendulum.datetime(
        2021, 11, 12, tz=pendulum.timezone("America/Toronto")
    ),
    "catchup": False,
    "tags": ["business reports"],
    "snowflake_conn_id": "snowflake_production",
    "schema": "KYC_PRODUCTION",
    "database": "ZETATANGO",
    "s3_conn_id": "s3_dataops",
    "s3_bucket": "ztportal-upload-production",
}


dag = DAG(
    dag_id="business_reports_import",
    description="A workflow to import business reports from s3 to snowflake",
    schedule_interval="0 */2 * * *",
    max_active_runs=1,
    catchup=False,
    template_searchpath="dags/sql",
    default_args=default_args,
)
dag.doc_md = __doc__


def _download_business_report(
    s3_bucket: str,
    engine: sqlalchemy.engine.Engine,
    target_table: Table,
    ts: str,
    s3_client,
    kms_client,
    s3_file_key: str,
    file_number: int,
) -> None:

    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_file_key)
    body = json.loads(response["Body"].read())
    body_decrypted = str(
        SymmetricPorky(kms_client).decrypt(
            enciphered_dek=b64decode(body["key"], b"-_"),
            enciphered_data=b64decode(body["data"], b"-_"),
            nonce=b64decode(body["nonce"], b"-_"),
        ),
        "utf-8",
    )
    logging.info(
        f"🔑 (#{file_number}) Decrypted business report located in key={s3_file_key}, bucket={s3_bucket}..."
    )
    with engine.begin() as tx:
        tx.execute(
            target_table.insert(),
            {
                "lookup_key": s3_file_key,
                "raw_response": body_decrypted,
                "last_modified_at": response["LastModified"],
                "batch_import_timestamp": ts,
            },
        )
        logging.info(
            f"✨️️ (#{file_number}) Successfully stored s3_file_key={s3_file_key}, s3_bucket={s3_bucket}"
            f"in {target_table} table..."
        )


def _download_all_business_reports(
    snowflake_conn_id: str,
    schema: str,
    s3_bucket: str,
    ts: str,
    **kwargs: None,
) -> None:

    boto_session = kwargs["task_session"]
    s3_client = boto_session.client("s3")
    kms_client = boto_session.client("kms")

    engine = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_sqlalchemy_engine()
    metadata_obj = MetaData()
    entities_business_reports = Table(
        "entities_business_reports",
        metadata_obj,
        autoload_with=engine,
        schema=schema,
    )
    raw_business_report_responses = Table(
        "raw_business_report_responses",
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

    download_business_report = partial(
        _download_business_report,
        s3_bucket,
        engine,
        raw_business_report_responses,
        ts,
        s3_client,
        kms_client,
    )

    with engine.connect() as conn:
        result = [r[0] for r in conn.execute(stmt)]
        logging.info(f"📂 Processing {len(result)} business_reports...")
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i, file_key in enumerate(result):
                executor.submit(
                    download_business_report,
                    s3_file_key=file_key,
                    file_number=i + 1,
                )
        duration = time.time() - start_time
        logging.info(
            f"⏱ Processed {len(result)} business reports in {duration} seconds"
        )


create_target_table = SnowflakeOperator(
    task_id="create_target_table",
    sql="business_reports/create_table.sql",
    params={"table_name": "raw_business_report_responses"},
    dag=dag,
)

download_business_reports = RBACPythonOperator(
    task_id="download_business_reports",
    python_callable=_download_all_business_reports,
    provide_context=True,
    op_kwargs=default_args,
    task_iam_role_arn="arn:aws:iam::810110616880:role/Access-ZtPortalUploadProduction-S3-Bucket-In-Zetatango-Prod-AWS",
    dag=dag,
)

create_target_table >> download_business_reports
