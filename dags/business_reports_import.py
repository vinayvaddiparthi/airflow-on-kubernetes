"""
#### Description
This workflow imports various business reports stored in the ztportal-upload-production s3 bucket.
"""
import logging
import json
import pendulum
import pandas as pd
import boto3
import sqlalchemy
from sqlalchemy import Table, MetaData, VARCHAR
from sqlalchemy.sql import select, func, text, literal_column, literal, join
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
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
)
dag.doc_md = __doc__

is_prod = Variable.get("environment") == "production"
SNOWFLAKE_CONN = "snowflake_production"
SCHEMA = f"CORE_{'PRODUCTION' if is_prod else 'STAGING'}"
S3_CONN = f"s3_dataops{'' if is_prod else '_staging'}"
S3_BUCKET = f"ztportal-upload-{'production' if is_prod else 'staging'}"

"""
create or table if it does not exist
list files on s3 bucket
download files
load to Snowflake
"""


def _list_files(
    snowflake_connection: str,
    schema: str,
    s3_bucket: str,
    **_: None,
) -> None:
    engine = SnowflakeHook(snowflake_conn_id=snowflake_connection).get_sqlalchemy_engine()
    metadata_obj = MetaData()


create_target_table = SnowflakeOperator(
    task_id="create_target_table",
    sql=
)

list_files = PythonOperator(
    task_id="list_files",
    python_callable=_list_files,
    provide_context=True,
    op_kwargs={
        "snowflake_connection": SNOWFLAKE_CONN,
        "schema": SCHEMA,
        "s3_bucket": S3_BUCKET,
    },
    dag=dag,
)
