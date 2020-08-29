from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from equifax_extras.models.core import Loan
from equifax_extras.consumer import RequestFile
import equifax_extras.utils.snowflake as snowflake

from typing import Any


cwd = os.getcwd()
tmp = os.path.join(cwd, "tmp")
output_dir = os.path.join(tmp, "equifax_batch")
Path(output_dir).mkdir(exist_ok=True)


def get_snowflake_engine() -> Engine:
    snowflake_kwargs = {
        "role": "ANALYST_ROLE",
        # "database": "ANALYTICS_PRODUCTION",
        "database": "ANALYTICS_REVIEW-BVROO-MERC-6W3DLF",
        "schema": "DBT_ARIO",
    }
    environment = Variable.get("environment", "")
    if environment == "development":
        snowflake_engine = snowflake.get_local_engine(
            "snowflake_analytics_production", snowflake_kwargs
        )
    else:
        snowflake_engine = snowflake.get_engine(
            "snowflake_analytics_production", snowflake_kwargs
        )
    return snowflake_engine


def generate_file(**context: Any) -> None:
    engine = get_snowflake_engine()
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    run_id = context["dag_run"].run_id
    file_name = f"equifax_batch_consumer_request_{run_id}.txt"
    file_path = os.path.join(output_dir, file_name)

    request_file = RequestFile(file_path)
    request_file.write_header()

    # For each Loan in state "repaying", write its associated Applicants to the request file
    repaying_loans = session.query(Loan).filter(Loan.state == "repaying").all()
    for repaying_loan in repaying_loans:
        merchant = repaying_loan.merchant.kyc_merchant
        applicants = merchant.owners
        for applicant in applicants:
            request_file.append(applicant, repaying_loan)

    request_file.write_footer()


def upload_file(**context: Any) -> None:
    s3 = S3Hook(aws_conn_id="s3_datalake")

    run_id = context["dag_run"].run_id
    file_name = f"equifax_batch_consumer_request_{run_id}.txt"
    file_path = os.path.join(output_dir, file_name)

    bucket_name = Variable.get("AWS_S3_EQUIFAX_BATCH_BUCKET_NAME")
    bucket = s3.get_bucket(bucket_name)
    remote_path = f"equifax_automated_batch/request/consumer/{file_name}"
    print(f"Uploading {file_path} to S3: {bucket_name}/{remote_path}")
    bucket.upload_file(file_path, remote_path)


def delete_file(**context: Any) -> None:
    run_id = context["dag_run"].run_id
    file_name = f"equifax_batch_consumer_request_{run_id}.txt"
    file_path = os.path.join(output_dir, file_name)

    os.remove(file_path)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1, 00, 00, 00),
    "concurrency": 1,
    "retries": 0,
}


with DAG(
    dag_id="equifax_batch_consumer_request",
    catchup=False,
    default_args=default_args,
    schedule_interval="@once",
) as dag:
    op_generate_file = PythonOperator(
        task_id="generate_file",
        python_callable=generate_file,
        execution_timeout=timedelta(hours=3),
        provide_context=True,
    )
    op_upload_file = PythonOperator(
        task_id="upload_file",
        python_callable=upload_file,
        provide_context=True,
    )
    op_delete_file = PythonOperator(
        task_id="delete_file",
        python_callable=delete_file,
        provide_context=True,
    )

op_generate_file >> op_upload_file >> op_delete_file


if __name__ == "__main__":
    from collections import namedtuple
    import random

    MockDagRun = namedtuple("MockDagRun", ["run_id"])
    mock_context = {"dag_run": MockDagRun(random.randint(10000, 99999))}

    generate_file(**mock_context)
    upload_file(**mock_context)
    delete_file(**mock_context)
