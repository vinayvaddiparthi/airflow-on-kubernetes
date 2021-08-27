"""
#### Description
This workflow sends the Equifax consumer request file (i.e. eligible applicant information) to
Equifax on a monthly basis for recertification purposes.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pendulum
import logging
from datetime import timedelta
from typing import Dict

from utils.failure_callbacks import slack_dag, sensor_timeout
from utils.gpg import init_gnupg

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["enterprisedata@thinkingcapital.ca"],
    "email_on_failure": False,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_dag("slack_data_alerts"),
    "start_date": pendulum.datetime(2021, 1, 1, tzinfo=pendulum.timezone("America/Toronto")),
    "catchup": False,
    "tags": ["equifax"],
    "description": "A workflow to send the consumer batch request file to Equifax",
}

dag = DAG(
    dag_id="equifax_consumer_outbox",
    default_args=default_args,
    schedule_interval="@daily",
)
dag.doc_md = __doc__

s3_connection = "s3_dataops"
sftp_connection = "equifax_sftp"
S3_BUCKET = "tc-data-airflow-production"


def encrypt_request_file(
    s3_conn: str,
    bucket_name: str,
    download_key: str,
    upload_key: str,
) -> None:
    s3 = S3Hook(aws_conn_id=s3_conn)
    filename = s3.download_file(key=download_key, bucket_name=bucket_name)
    with open(filename, "rb") as reader:
        gpg = init_gnupg()
        encrypted_message = gpg.encrypt_file(
            reader, "sts@equifax.com", always_trust=True
        )
    with open(filename, "wb") as writer:
        writer.write(encrypted_message.data)
        s3.load_file(
            filename=filename, key=upload_key, bucket_name=bucket_name, replace=True
        )


def _mark_request_as_sent(context: Dict) -> None:
    Variable.set("equifax_consumer_request_sent", True)
    logging.info(context["task_instance"].log_url)
    logging.info("Request file successfully sent to Equifax")


def _check_if_file_sent() -> bool:
    return False if Variable.get("equifax_consumer_request_sent") == "True" else True


task_check_if_file_sent = ShortCircuitOperator(
    task_id="check_if_file_sent",
    python_callable=_check_if_file_sent,
    dag=dag,
)

task_is_request_file_available = S3KeySensor(
    task_id="is_request_file_available",
    bucket_key=f"s3://{S3_BUCKET}/equifax/consumer/request_reviewed_by_risk/{{ var.value.equifax_consumer_request_filename }}",
    aws_conn_id=s3_connection,
    poke_interval=5,
    timeout=20,
    on_failure_callback=sensor_timeout,
    dag=dag,
)

task_encrypt_request_file = PythonOperator(
    task_id="encrypt_request_file",
    python_callable=encrypt_request_file,
    op_kwargs={
        "s3_conn": s3_connection,
        "bucket_name": S3_BUCKET,
        "download_key": "equifax/consumer/request_reviewed_by_risk/{{ var.value.equifax_consumer_request_filename }}",
        "upload_key": "equifax/consumer/outbox/{{ var.value.equifax_consumer_request_filename }}.pgp",
    },
    dag=dag,
)

task_is_outbox_file_available = S3KeySensor(
    task_id="is_outbox_file_available",
    bucket_key=f"s3://{S3_BUCKET}/equifax/consumer/outbox/{{ var.value.equifax_consumer_request_filename }}.pgp",
    aws_conn_id=s3_connection,
    poke_interval=5,
    timeout=20,
    on_failure_callback=sensor_timeout,
    dag=dag,
)

task_create_s3_to_sftp_job = S3ToSFTPOperator(
    task_id="create_s3_to_sftp_job",
    sftp_conn_id=sftp_connection,
    sftp_path="inbox/{{ var.value.equifax_consumer_request_filename }}.pgp",
    s3_conn_id=s3_connection,
    s3_bucket=S3_BUCKET,
    s3_key="equifax/consumer/outbox/{{ var.value.equifax_consumer_request_filename }}.pgp",
    on_success_callback=_mark_request_as_sent,
    dag=dag,
)

(
    task_check_if_file_sent
    >> task_is_request_file_available
    >> task_encrypt_request_file
    >> task_is_outbox_file_available
    >> task_create_s3_to_sftp_job
)
