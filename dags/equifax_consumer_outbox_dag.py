"""
#### Description
This workflow sends the Equifax consumer request file (i.e. eligible applicant information) to
Equifax on a monthly basis for recertification purposes.
"""
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSensorTimeout

from datetime import datetime, timedelta
from typing import List

from utils.failure_callbacks import slack_dag
from utils.gpg import _init_gnupg

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["enterprisedata@thinkingcapital.ca"],
    "email_on_failure": False,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_dag("slack_data_alerts"),
    "start_date": datetime(2021, 1, 1),
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


def _failure_callback(context):
    if isinstance(context["exception"], AirflowSensorTimeout):
        print(context)
        print("Sensor timed out")


def download_file_from_s3(
    s3_conn: str, bucket_name: str, key: str, **_: None
) -> List[str]:
    s3 = S3Hook(aws_conn_id=s3_conn)
    filename = s3.download_file(key=key, bucket_name=bucket_name)
    return filename


def encrypt_request_file(
    task_instance: TaskInstance,
    **_: None,
) -> str:
    filename = task_instance.xcom_pull("download_request_file")
    gpg = _init_gnupg()
    with open(filename, "rb+") as file:
        encrypted_message = gpg.encrypt_file(file, "sts@equifax.com", always_trust=True)
        file.write(encrypted_message.data)
        return filename


def upload_file_to_s3(
    bucket_name: str,
    key: str,
    task_instance: TaskInstance,
    **_: None,
):
    filename = task_instance.xcom_pull("encrypt_request_file")
    s3 = S3Hook(aws_conn_id=s3_connection)
    s3.load_file(
        filename=filename, key=key, bucket_name=bucket_name, replace=False, encrypt=True
    )


task_is_request_file_available = S3KeySensor(
    task_id="is_request_file_available",
    bucket_key="s3://tc-data-airflow-production/equifax/consumer/request/{{ var.value.equifax_consumer_request_filename }}",
    aws_conn_id=s3_connection,
    poke_interval=5,
    timeout=20,
    on_failure_callback=_failure_callback,
    dag=dag,
)

task_download_request_file = PythonOperator(
    task_id="download_request_file",
    python_callable=download_file_from_s3,
    op_kwargs={
        "s3_conn": s3_connection,
        "bucket_name": S3_BUCKET,
        "key": "equifax/consumer/request/{{ var.value.equifax_consumer_request_filename }}",
    },
    provide_context=True,
    dag=dag,
)

task_encrypt_request_file = PythonOperator(
    task_id="encrypt_request_file",
    python_callable=encrypt_request_file,
    provide_context=True,
    dag=dag,
)

task_upload_request_file = PythonOperator(
    task_id="upload_request_file",
    python_callable=upload_file_to_s3,
    op_kwargs={
        "bucket_name": S3_BUCKET,
        "key": "equifax/consumer/outbox/{{ var.value.equifax_consumer_request_filename }}.pgp",
    },
    provide_context=True,
    dag=dag,
)

task_is_outbox_file_available = S3KeySensor(
    task_id="is_outbox_file_available",
    bucket_key="s3://tc-data-airflow-production/equifax/consumer/outbox/{{ var.value.equifax_consumer_request_filename }}.pgp",
    aws_conn_id=s3_connection,
    poke_interval=5,
    timeout=20,
    on_failure_callback=_failure_callback,
    dag=dag,
)

task_create_s3_to_sftp_job = S3ToSFTPOperator(
    task_id="create_s3_to_sftp_job",
    sftp_conn_id=sftp_connection,
    sftp_path="inbox/{{ var.value.equifax_consumer_request_filename }}.pgp",
    s3_conn_id=s3_connection,
    s3_bucket=S3_BUCKET,
    s3_key="equifax/consumer/outbox/{{ var.value.equifax_consumer_request_filename }}.pgp",
    dag=dag,
)

(
    task_is_request_file_available
    >> task_download_request_file
    >> task_encrypt_request_file
    >> task_upload_request_file
    >> task_is_outbox_file_available
    >> task_create_s3_to_sftp_job
)
