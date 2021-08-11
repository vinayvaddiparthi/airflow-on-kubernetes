"""
# Equifax Consumer Outbox DAG

This workflow sends the Equifax consumer request file (i.e. eligible applicant information) to
Equifax on a monthly basis for recertification purposes.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowSensorTimeout

from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
import gnupg
import logging
from typing import IO, List, Any
from fs.sshfs import SSHFS
from fs.tools import copy_file_data
from fs_s3fs import S3FS

from helpers.suspend_aws_env import SuspendAwsEnvVar
from utils.failure_callbacks import slack_dag
from utils.gpg import _init_gnupg

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['enterprisedata@thinkingcapital.ca'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_dag('slack_data_alerts'),
    'start_date': datetime(2021, 1, 1),
    'catchup': False,
    'tags': ['equifax'],
    'description': "A workflow to send the consumer batch request file to Equifax",
}

dag = DAG(
    dag_id='equifax_consumer_outbox',
    default_args=default_args,
    schedule_interval="@daily",
)
dag.doc_md = __doc__

s3_connection = 's3_dataops'
sftp_connection = 'equifax_sftp'
S3_BUCKET = 'tc-data-airflow-production'
# S3_KEY = 'equifax/consumer/outbox/eqxds.exthinkingpd.ds.20210801.txt'
S3_KEY = 'equifax/consumer/outbox/eqxds.exthinkingpd.ds.20210801.test.txt'

# task: check if s3 folder (/request) contains request file for this month
# task: download request file from s3 and encrypt
# task: if the request file for this month exists, then encrypt the file and upload to s3
# task: if the request file for this month exists, then send the file to Equifax


def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
        print("Sensor timed out")


def download_file_from_s3(s3_conn: str, bucket_name: str, key: str, **_: Any) -> List[str]:
    s3 = S3Hook(aws_conn_id=s3_conn)
    # keys = s3.list_keys(bucket_name=bucket_name, prefix=prefix)
    # recert_date = datetime.strftime(datetime.strptime(ds_nodash, '%Y%m%d').replace(day=1), '%Y%m%d')
    # matches = [key for key in keys if key == f'{prefix}/eqxds.exthinkingpd.ds.{recert_date}.txt']
    # if len(matches) > 1:
    #     logging.error('More than one request file found for this month', matches)
    # if len(matches) == 0:
    #     logging.error('No matching request file found for this month', matches)
    filename = s3.download_file(key=key, bucket_name=bucket_name)
    return filename

def encrypt_request_file()
    filepath = xcom.


def upload_file_to_s3(filename: str, key: str, bucket_name: str):
    s3 = S3Hook(aws_conn_id=s3_connection)
    s3.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=False, encrypt=True)

def encrypt(fd: IO[bytes]) -> IO[bytes]:
    gpg = _init_gnupg()
    encrypted_message = gpg.encrypt_file(fd, "sts@equifax.com", always_trust=True)
    return BytesIO(encrypted_message.data)

# def _get_sshfs_from_conn(ssh_conn: str) -> SSHFS:
#     ssh_connection = SSHHook.get_connection(ssh_conn)
#
#     return SSHFS(
#         host=ssh_connection.host,
#         user=ssh_connection.login,
#         passwd=ssh_connection.password,
#     )


# def _get_s3fs_from_conn(aws_conn: str) -> S3FS:
#     aws_connection = AwsBaseHook.get_connection(aws_conn)
#
#     return S3FS(
#         bucket_name=aws_connection.extra_dejson["bucket"],
#         region=aws_connection.extra_dejson["region"],
#         dir_path=aws_connection.extra_dejson["dir_path"],
#         aws_access_key_id=aws_connection.extra_dejson["aws_access_key_id"],
#         aws_secret_access_key=aws_connection.extra_dejson["aws_secret_access_key"],
#     )


def sync_s3fs_to_sshfs(aws_conn: str, sshfs_conn: str) -> None:
    with SuspendAwsEnvVar():
        s3fs, sshfs = _get_s3fs_from_conn(aws_conn), _get_sshfs_from_conn(sshfs_conn)

        local_files = s3fs.listdir("outbox")

        for file in local_files:
            with s3fs.open(f"outbox/{file}", "rb") as origin_file, sshfs.open(
                f"inbox/{file}.pgp", "wb"
            ) as remote_file:
                encrypted = encrypt(origin_file)
                copy_file_data(encrypted, remote_file)
                s3fs.remove(f"outbox/{file}")


task_is_request_file_available = S3KeySensor(
    task_id='is_request_file_available',
    bucket_key='s3://tc-data-airflow-production/equifax/consumer/request/{{ var.value.equifax_consumer_request_filename }}',
    aws_conn_id=s3_connection,
    poke_interval=5,
    timeout=20,
    on_failure_callback=_failure_callback
    dag=dag,
)

task_download_request_file = PythonOperator(
    task_id='download_request_file',
    python_callable=download_file_from_s3,
    op_kwargs={
        's3_conn': s3_connection,
        'bucket_name': S3_BUCKET,
        'key': 'equifax/consumer/request/{{ var.value.equifax_consumer_request_filename }}'
    },
    provide_context=True,
    dag=dag,
)

task_upload_request_file = PythonOperator(
    task_id='upload_request_file',
    python_callable=upload_file_to_s3,
    op_kwargs={
        'filename': '/Users/jimkim/Documents/equifax/recert-august2021/eqxds.exthinkingpd.ds.20210801.test.txt',
        'key': S3_KEY,
        'bucket_name': S3_BUCKET,
    },
    dag=dag,
)

task_create_s3_to_sftp_job = S3ToSFTPOperator(
    task_id='create_s3_to_sftp_job',
    sftp_conn_id=sftp_connection,
    sftp_path='inbox/',
    s3_conn_id=s3_connection,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY,
    dag=dag,
)

task_sync_s3fs_to_sshfs = PythonOperator(
    task_id='sync_s3fs_to_sshfs',
    python_callable=sync_s3fs_to_sshfs,
    op_kwargs={
        "aws_conn": "s3_dataops",
        "sshfs_conn": "equifax_sftp",
    },
    dag=dag,
)
