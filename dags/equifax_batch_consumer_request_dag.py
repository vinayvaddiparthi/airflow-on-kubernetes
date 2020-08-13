from datetime import datetime, timedelta
import os
import sqlite3
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from equifax_extras.models import Applicant
from equifax_extras.consumer import RequestFile
import equifax_extras.utils.snowflake as snowflake

from typing import Optional


cwd = os.getcwd()
tmp = os.path.join(cwd, "tmp")
output_dir = os.path.join(tmp, "equifax_batch")
Path(output_dir).mkdir(exist_ok=True)

sqlite_db_path = os.path.join(output_dir, "tmp_database.db")
sqlite_db_url = f"sqlite:///{sqlite_db_path}"


def init_sqlite() -> None:
    print(f"Connecting to {sqlite_db_url}")
    conn = None
    try:
        conn = sqlite3.connect(sqlite_db_path)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def generate_file(**context: Optional[str]) -> None:
    sqlite_engine = create_engine(sqlite_db_url)
    session_maker = sessionmaker(bind=sqlite_engine)
    session = session_maker()

    run_id = context["dag_run"].run_id
    file_name = f"equifax_batch_consumer_request_{run_id}.txt"
    file_path = os.path.join(output_dir, file_name)

    num_applicants = session.query(Applicant).count()
    print(f"Applicant count: {num_applicants}")

    r = RequestFile(file_path)
    r.write_header()
    applicants = session.query(Applicant).all()
    for applicant in applicants:
        r.append(applicant)
    r.write_footer()


def upload_file(**context: Optional[str]) -> None:
    s3 = S3Hook(aws_conn_id="s3_connection")

    run_id = context["dag_run"].run_id
    file_name = f"equifax_batch_consumer_request_{run_id}.txt"
    file_path = os.path.join(output_dir, file_name)

    bucket_name = Variable.get("AWS_S3_EQUIFAX_BATCH_BUCKET_NAME")
    bucket = s3.get_bucket(bucket_name)
    remote_path = f"consumer/request/{file_name}"
    print(f"Uploading {file_path} to S3: {bucket_name}/{remote_path}")
    bucket.upload_file(file_path, remote_path)


def get_snowflake_engine() -> Engine:
    environment = Variable.get("environment", "")
    if environment == "development":
        snowflake_engine = snowflake.get_local_engine("snowflake_conn")
    else:
        snowflake_engine = snowflake.get_engine("snowflake_conn")
    return snowflake_engine


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1, 10, 00, 00),
    "concurrency": 1,
    "retries": 0,
}


with DAG(
    dag_id="equifax_batch_consumer_request",
    catchup=False,
    default_args=default_args,
    schedule_interval="@once",
) as dag:
    op_init_sqlite = PythonOperator(task_id="init_sqlite", python_callable=init_sqlite,)
    op_load_addresses = PythonOperator(
        task_id="load_addresses",
        python_callable=snowflake.load_addresses,
        op_kwargs={
            "remote_engine": get_snowflake_engine(),
            "local_engine": create_engine(sqlite_db_url),
        },
    )
    op_load_address_relationships = PythonOperator(
        task_id="load_address_relationships",
        python_callable=snowflake.load_address_relationships,
        op_kwargs={
            "remote_engine": get_snowflake_engine(),
            "local_engine": create_engine(sqlite_db_url),
        },
    )
    op_load_applicants = PythonOperator(
        task_id="load_applicants",
        python_callable=snowflake.load_applicants,
        op_kwargs={
            "remote_engine": get_snowflake_engine(),
            "local_engine": create_engine(sqlite_db_url),
        },
    )
    op_load_applicant_attributes = PythonOperator(
        task_id="load_applicant_attributes",
        python_callable=snowflake.load_applicant_attributes,
        op_kwargs={
            "remote_engine": get_snowflake_engine(),
            "local_engine": create_engine(sqlite_db_url),
        },
    )
    op_generate_file = PythonOperator(
        task_id="generate_file",
        python_callable=generate_file,
        execution_timeout=timedelta(hours=3),
        provide_context=True,
    )
    op_upload_file = PythonOperator(
        task_id="upload_file", python_callable=upload_file, provide_context=True,
    )

load = [
    op_load_addresses,
    op_load_address_relationships,
    op_load_applicants,
    op_load_applicant_attributes,
]
op_init_sqlite >> load >> op_generate_file >> op_upload_file


if __name__ == "__main__":
    from collections import namedtuple
    import random

    MockDagRun = namedtuple("MockDagRun", ["run_id"])
    mock_context = {"dag_run": MockDagRun(random.randint(10000, 99999))}

    init_sqlite()
    sqlite_engine = create_engine(sqlite_db_url)
    snowflake_engine = get_snowflake_engine()
    snowflake.load_applicants(snowflake_engine, sqlite_engine)
    snowflake.load_applicant_attributes(snowflake_engine, sqlite_engine)
    snowflake.load_addresses(snowflake_engine, sqlite_engine)
    snowflake.load_address_relationships(snowflake_engine, sqlite_engine)
    generate_file(**mock_context)
    upload_file(**mock_context)
