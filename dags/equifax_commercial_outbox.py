"""
#### Description
This workflow sends the Equifax commercial request file (i.e. eligible merchant information) to
Equifax on every odd month for recertification purposes.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator

import logging
import pendulum
from datetime import timedelta
from typing import Dict

from utils.failure_callbacks import slack_dag, sensor_timeout
from utils.gpg import init_gnupg

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_dag("slack_data_alerts"),
    "start_date": pendulum.datetime(
        2021, 9, 1, tzinfo=pendulum.timezone("America/Toronto")
    ),
    "catchup": False,
    "tags": ["equifax"],
    "description": "A workflow to send the commercial batch request file to Equifax",
}

dag = DAG(
    dag_id="equifax_commercial_outbox",
    schedule_interval="@daily",
    default_args=default_args,
)
dag.doc_md = __doc__

S3_CONN = "s3_dataops"
S3_BUCKET = "tc-data-airflow-production"
DIR_PATH = "equifax/commercial"
SFTP_CONN = "equifax_sftp"


def _check_if_file_sent() -> bool:
    return False if Variable.get("equifax_commercial_request_sent") == "True" else True


def _mark_request_as_sent(context: Dict) -> None:
    Variable.set("equifax_commercial_request_sent", True)
    logging.info(context["task_instance"].log_url)
    logging.info("Commercial request file successfully sent to Equifax")


def _encrypt_request_file() -> None:
    s3 = S3Hook(aws_conn_id=S3_CONN)
    filename = s3.download_file(
        key=f"{DIR_PATH}/request/{{{{ var.value.equifax_commercial_request_filename }}}}",
        bucket_name=S3_BUCKET,
    )
    with open(filename, "rb") as reader:
        gpg = init_gnupg()
        encrypted_msg = gpg.encrypt_file(
            stream=reader,
            recipients="sts@equifax.com",
            always_trust=True,
        )
    with open(filename, "wb") as writer:
        writer.write(encrypted_msg.data)
        s3.load_file(
            filename=filename,
            key=f"{DIR_PATH}/outbox/{{{{ var.value.equifax_commercial_request_filename }}}}.pgp",
            bucket_name=S3_BUCKET,
            replace=True,
        )


check_if_file_sent = ShortCircuitOperator(
    task_id="check_if_file_sent",
    python_callable=_check_if_file_sent,
    do_xcom_push=False,
    dag=dag,
)

is_request_file_available = S3KeySensor(
    task_id="is_request_file_available",
    bucket_key=f"s3://{S3_BUCKET}/{DIR_PATH}/request/{{{{ var.value.equifax_commercial_request_filename }}}}",
    aws_conn_id=S3_CONN,
    poke_interval=5,
    timeout=20,
    on_failure_callback=sensor_timeout,
    dag=dag,
)

encrypt_request_file = PythonOperator(
    task_id="encrypt_request_file",
    python_callable=_encrypt_request_file,
    dag=dag,
)

create_s3_to_sftp_job = S3ToSFTPOperator(
    task_id="create_s3_to_sftp_job",
    sftp_conn_id=SFTP_CONN,
    sftp_path="inbox/{{ var.value.equifax_commercial_request_filename }}.pgp",
    s3_conn_id=S3_CONN,
    s3_bucket=S3_BUCKET,
    s3_key=f"{DIR_PATH}/outbox/{{{{ var.value.equifax_commercial_request_filename }}}}.pgp",
    on_success_callback=_mark_request_as_sent,
    dag=dag,
)

(
    check_if_file_sent
    >> is_request_file_available
    >> encrypt_request_file
    >> create_s3_to_sftp_job
)
