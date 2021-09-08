"""
### Description
This workflow processes the commercial response file from Equifax. Once the response file is downloaded, the encrypted
response file will be processed and uploaded to Snowflake.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

import datetime
import logging
import pendulum
from io import BytesIO
from typing import IO, Any
from fs.sshfs import SSHFS
from fs.tools import copy_file_data
from fs_s3fs import S3FS
import pyarrow.csv as pv, pyarrow.parquet as pq
from pendulum import Pendulum
from pyarrow._csv import ReadOptions
from pyarrow.lib import ArrowInvalid, array

from helpers.suspend_aws_env import SuspendAwsEnvVar
from utils.failure_callbacks import slack_dag
from utils.gpg import init_gnupg

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(
        2020, 11, 15, tzinfo=pendulum.timezone("America/Toronto")
    ),
    "retries": 0,
    "catchup": False,
    "on_failure_callback": slack_dag("slack_data_alerts"),
    "tags": ["equifax"],
    "description": "A workflow to download and process the commercial batch response file from Equifax",
}

dag = DAG(
    dag_id="equifax_commercial_inbox",
    schedule_interval="@daily",
    default_args=default_args,
)
dag.doc_md = __doc__

SFTP_CONN = "equifax_sftp"
SNOWFLAKE_CONN = "airflow_production"
S3_BUCKET = f"tc-data-airflow-{'production' if Variable.get('environment') == 'production' else 'staging'}"
COMMERCIAL_FILENAME = ""
EXECUTOR_CONFIG = {
    "KubernetesExecutor": {
        "annotations": {
            "iam.amazonaws.com/role": "arn:aws:iam::810110616880:role/"
            "KubernetesAirflowProductionEquifaxCommercialRole"
        },
    },
    "resources": {
        "requests": {"memory": "512Mi"},
    },
}


def _check_if_file_downloaded() -> bool:
    return Variable.get("equifax_commercial_response_downloaded") != "True"


def _check_if_response_available(ti: TaskInstance, **_: None) -> bool:
    hook = SFTPHook(ftp_conn_id=SFTP_CONN)
    files = hook.list_directory(path="outbox/")
    if len(files) == 0:
        return False
    # can safely assume only 2 commercial files (e.g., dv and risk) will be available every odd month as Equifax clears
    # the directory after 7 days
    filenames = [file for file in files if file.startswith("exthinkingpd.eqxcan.eqxcom")]
    if len(filenames) != 2:  # wait until both commercial files are ready
        return False
    filenames_no_filetype = []
    for filename in filenames:
        filename_parts = filename.split(".")
        filename_parts_no_filetype = filename_parts[:-2]
        filename_no_filetype = ".".join(filename_parts_no_filetype)
        filenames_no_filetype.append(filename_no_filetype)
    logging.info(f"Following commercial response files are available to download: {filenames_no_filetype}")
    ti.xcom_push(key="filenames", value=filenames_no_filetype)
    return True


def _get_sshfs_from_conn(ssh_conn: str) -> SSHFS:
    ssh_connection = SSHHook.get_connection(ssh_conn)

    return SSHFS(
        host=ssh_connection.host,
        user=ssh_connection.login,
        passwd=ssh_connection.password,
    )


def _get_s3fs_from_conn(aws_conn: str) -> S3FS:
    aws_connection = AwsBaseHook.get_connection(aws_conn)

    return S3FS(
        bucket_name=aws_connection.extra_dejson["bucket"],
        region=aws_connection.extra_dejson["region"],
        dir_path=aws_connection.extra_dejson["dir_path"],
        aws_access_key_id=aws_connection.extra_dejson["aws_access_key_id"],
        aws_secret_access_key=aws_connection.extra_dejson["aws_secret_access_key"],
    )


def _sync_sshfs_to_s3fs(aws_conn: str, sshfs_conn: str) -> None:
    with SuspendAwsEnvVar():
        s3fs, sshfs = _get_s3fs_from_conn(aws_conn), _get_sshfs_from_conn(sshfs_conn)

        remote_files = sshfs.listdir("outbox")
        commercial_remote_files = [
            file
            for file in remote_files
            if file.startswith("exthinkingpd.eqxcan.eqxcom")
        ]
        local_files = set(s3fs.listdir("inbox"))  # cast to set for O(1) lookup

        for file in (
            file for file in commercial_remote_files if file not in local_files
        ):
            with sshfs.open(f"outbox/{file}", "rb") as origin_file, s3fs.open(
                f"inbox/{file}", "wb"
            ) as dest_file:
                copy_file_data(origin_file, dest_file)


def _decrypt_received_files(aws_conn: str) -> None:
    with SuspendAwsEnvVar():
        s3fs = _get_s3fs_from_conn(aws_conn)

        for file in s3fs.listdir("inbox"):
            with s3fs.open(f"inbox/{file}", "rb") as encrypted_file, s3fs.open(
                f"decrypted/{file}"[:-4], "wb"
            ) as decrypted_file:
                decrypted = decrypt(encrypted_file)
                copy_file_data(decrypted, decrypted_file)


def _decode_decrypted_files(
    aws_conn: str, execution_date: Pendulum, run_id: str, **kwargs: Any
) -> None:
    with SuspendAwsEnvVar():
        s3fs = _get_s3fs_from_conn(aws_conn)

        for file in (
            file
            for file in s3fs.listdir("decrypted")
            if f"{file[:-6]}.parquet" not in set(s3fs.listdir("parquet"))
        ):
            with s3fs.open(f"decrypted/{file}", "rb") as decrypted_file, s3fs.open(
                f"parquet/{file[:-6]}.parquet", "wb"
            ) as parquet_file:
                try:
                    table_ = pv.read_csv(
                        decrypted_file,
                        read_options=ReadOptions(block_size=8388608),
                    )

                    table_ = table_.append_column(
                        "__execution_date",
                        array([execution_date.to_iso8601_string()] * len(table_)),
                    ).append_column("__run_id", array([run_id] * len(table_)))

                    if table_.num_rows == 0:
                        logging.warning(f"📝️ Skipping empty file {decrypted_file}")
                        continue

                    pq.write_table(table_, parquet_file)

                except ArrowInvalid as exc:
                    logging.error(f"❌ Failed to read file {decrypted_file.name}: {exc}")

                logging.info(f"📝️ Converted file {decrypted_file.name}")


def _create_table_from_stage(snowflake_conn: str, schema: str, stage: str) -> None:
    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    qualified_table = f"{schema}.{stage}"

    with engine.begin() as tx:
        stmt = f"select $1 as fields from @{qualified_table}"  # nosec

        tx.execute(
            f"create or replace transient table {qualified_table} as {stmt}"  # nosec
        ).fetchall()


def encrypt(fd: IO[bytes]) -> IO[bytes]:
    gpg = init_gnupg()
    encrypted_message = gpg.encrypt_file(fd, "sts@equifax.com", always_trust=True)
    return BytesIO(encrypted_message.data)


def decrypt(fd: IO[bytes]) -> IO[bytes]:
    gpg = init_gnupg()
    pass_phrase = Variable.get("equifax_pgp_passphrase", deserialize_json=False)
    decrypted_message = gpg.decrypt_file(fd, always_trust=True, passphrase=pass_phrase)
    return BytesIO(decrypted_message.data)


check_if_file_downloaded = ShortCircuitOperator(
    task_id="check_if_files_downloaded",
    python_callable=_check_if_file_downloaded,
    do_xcom_push=False,
    dag=dag,
)

check_if_response_available = ShortCircuitOperator(
    task_id="check_if_response_available",
    python_callable=_check_if_response_available,
    provide_context=True,
    dag=dag,
)

sync_sshfs_to_s3fs = PythonOperator(
    task_id="sync_sshfs_to_s3fs",
    python_callable=_sync_sshfs_to_s3fs,
    op_kwargs={
        "aws_conn": "s3_equifax_commercial",
        "sshfs_conn": "ssh_equifax_commercial",
    },
    retry_delay=datetime.timedelta(hours=1),
    retries=3,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

decrypt_received_files = PythonOperator(
    task_id="decrypt_received_files",
    python_callable=_decrypt_received_files,
    op_kwargs={
        "aws_conn": "s3_equifax_commercial",
    },
    retry_delay=datetime.timedelta(hours=1),
    retries=3,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

decode_decrypted_files = PythonOperator(
    task_id="convert_to_parquet",
    python_callable=_decode_decrypted_files,
    op_kwargs={
        "aws_conn": "s3_equifax_commercial",
    },
    provide_context=True,
    retry_delay=datetime.timedelta(hours=1),
    retries=3,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

create_table_from_stage = PythonOperator(
    task_id="create_table_from_stage",
    python_callable=_create_table_from_stage,
    op_kwargs={
        "snowflake_conn": "airflow_production",
        "schema": "airflow.production",
        "stage": "equifax_commercial_inbox",
    },
    retry_delay=datetime.timedelta(hours=1),
    retries=3,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

(
    check_if_file_downloaded
    >> check_if_response_available
    >> sync_sshfs_to_s3fs
    >> decrypt_received_files
    >> decode_decrypted_files
    >> create_table_from_stage
)
