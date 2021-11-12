"""
#### Description
This workflow imports various business reports stored in the ztportal-upload-production s3 bucket.
"""
import time
import logging
import json
import pendulum
import pandas as pd
import asyncio
from aiobotocore.client import BaseClient
from aiobotocore.session import get_session
from datetime import timedelta
from sqlalchemy import Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.sql import select, func, text, literal_column
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from airflow import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
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


async def _download_business_report(
    client: BaseClient,
    engine: Engine,
    metadata: MetaData,
    s3_file_key: str,
    schema: str,
    s3_conn_id: str,
    s3_bucket: str,
    ts: str,
    **_: None,
) -> None:
    # s3 = S3Hook(aws_conn_id=s3_conn_id)
    # s3_object = s3.get_key(key=s3_file_key, bucket_name=s3_bucket)

    # get object from s3
    response = await client.get_object(Bucket=s3_bucket, Key=s3_file_key)
    # ensure the connection is correctly re-used/closed
    async with response["Body"] as stream:
        body = await stream.read()
        body_json = json.loads(body)
        body_decrypted = str(
            SymmetricPorky(aws_region="ca-central-1").decrypt(
                enciphered_dek=b64decode(body_json["key"], b"-_"),
                enciphered_data=b64decode(body_json["data"], b"-_"),
                nonce=b64decode(body_json["nonce"], b"-_"),
            ),
            "utf-8",
        )

        logging.info(
            f"🔑 Decrypted business report located in key={s3_file_key}, bucket={s3_bucket}..."
        )

        raw_business_report_responses = Table(
            "raw_business_report_responses",
            metadata,
            autoload_with=engine,
            schema=schema,
        )

        with engine.begin() as tx:
            batch_import_timestamp = ts
            select_query = select(
                columns=[
                    literal_column(f"'{s3_file_key}'").label("lookup_key"),
                    func.parse_json(body_decrypted).label("raw_response"),
                    literal_column(f"'{response['LastModified']}'").label(
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

            # TODO: investigate why this log only appears once in the threading approach
            logging.info(
                f"❄️️ Successfully inserted {s3_file_key} to {raw_business_report_responses} table..."
            )


async def _download_all_business_reports(
    snowflake_conn_id: str,
    schema: str,
    s3_conn_id: str,
    s3_bucket: str,
    ts: str,
    num_threads: int=5,
    **_: None,
) -> None:
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

    df_subset = df_reports_to_download.iloc[:5]
    logging.info(f"📂 Processing {df_subset.size} business_reports...")

    start_time = time.time()

    """synchronous processing"""
    # for _, row in df_subset.iterrows():
    #     _download_business_report(
    #         engine,
    #         metadata,
    #         row[0],
    #         schema=schema,
    #         s3_conn_id=s3_conn_id,
    #         s3_bucket=s3_bucket,
    #         ts=ts,
    #     )

    """thread processing"""
    # with ThreadPoolExecutor(max_workers=num_threads) as executor:
    #     for _, row in df_subset.iterrows():
    #         executor.submit(
    #             _download_business_report,
    #             engine,
    #             metadata,
    #             row[0],
    #             schema=schema,
    #             s3_conn_id=s3_conn_id,
    #             s3_bucket=s3_bucket,
    #             ts=ts,
    #         )

    aws_hook = AwsBaseHook(aws_conn_id=s3_conn_id, client_type="s3")
    aws_credentials = aws_hook.get_credentials()

    """asyncio"""
    session = get_session()
    async with session.create_client(
        "s3",
        region_name="ca-central-1",
        aws_access_key_id=aws_credentials.access_key,
        aws_secret_access_key=aws_credentials.secret_key,
    ) as client:
        tasks = []
        for _, row in df_subset.iterrows():
            task = asyncio.ensure_future(
                _download_business_report(
                    client,
                    engine,
                    metadata,
                    schema=schema,
                    s3_conn_id=s3_conn_id,
                    s3_bucket=s3_bucket,
                    ts=ts,
                    s3_file_key=row[0],
                )
            )
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

    duration = time.time() - start_time
    logging.info(
        f"⏱ Downloaded {df_subset.size} business reports in {duration} seconds"
    )


def _task_callable(
    snowflake_conn_id: str,
    schema: str,
    s3_conn_id: str,
    s3_bucket: str,
    ts: str,
    num_threads: int = 5,
    **_: None,
):
    asyncio.get_event_loop().run_until_complete(
        _download_all_business_reports(
            snowflake_conn_id,
            schema,
            s3_conn_id,
            s3_bucket,
            ts,
            num_threads,
        )
    )


create_target_table = SnowflakeOperator(
    task_id="create_target_table",
    sql="business_reports/create_table.sql",
    params={"table_name": "raw_business_report_responses"},
    schema=SCHEMA,
    database="ZETATANGO",
    snowflake_conn_id=SNOWFLAKE_CONN,
    dag=dag,
)

download_business_reports = PythonOperator(
    task_id="download_business_reports",
    # python_callable=_download_all_business_reports,
    python_callable=_task_callable,
    provide_context=True,
    op_kwargs={
        "snowflake_conn_id": SNOWFLAKE_CONN,
        "schema": SCHEMA,
        "s3_conn_id": "s3_dataops",
        "s3_bucket": "ztportal-upload-production",
        "num_threads": 10,
    },
    dag=dag,
)

create_target_table >> download_business_reports

# if __name__ == "__main__":
#     start_time = time.time()
#     _download_all_business_reports(
#         snowflake_conn_id=SNOWFLAKE_CONN,
#         schema=SCHEMA,
#         s3_conn_id="s3_dataops",
#         s3_bucket="ztportal-upload-production",
#         ts="2021-11-11T00:00:00Z",
#     )
#     duration = time.time() - start_time
#     logging.info(f"⏱ Downloaded in {duration} seconds")
