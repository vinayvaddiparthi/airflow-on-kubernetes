import datetime
import logging
from io import BytesIO

from pathlib import Path
from typing import IO, List, Any

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from fs.sshfs import SSHFS
from fs.tools import copy_file_data
from fs_s3fs import S3FS

import pyarrow.csv as pv, pyarrow.parquet as pq
from pendulum import Pendulum
from pyarrow._csv import ReadOptions
from pyarrow.lib import ArrowInvalid, array

from helpers.suspend_aws_env import SuspendAwsEnvVar

import gnupg

from utils.failure_callbacks import slack_dag


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


def _init_gnupg() -> gnupg.GPG:
    path_ = Path("~/.gnupg")
    path_.mkdir(parents=True, exist_ok=True)
    gpg = gnupg.GPG(gnupghome=path_)

    keys: List[str] = Variable.get("equifax_pgp_keys", deserialize_json=True)
    for key in keys:
        gpg.import_keys(key)

    return gpg


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


def sync_sshfs_to_s3fs(aws_conn: str, sshfs_conn: str) -> None:
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


def decrypt_received_files(aws_conn: str) -> None:
    with SuspendAwsEnvVar():
        s3fs = _get_s3fs_from_conn(aws_conn)

        for file in s3fs.listdir("inbox"):
            with s3fs.open(f"inbox/{file}", "rb") as encrypted_file, s3fs.open(
                f"decrypted/{file}"[:-4], "wb"
            ) as decrypted_file:
                decrypted = decrypt(encrypted_file)
                copy_file_data(decrypted, decrypted_file)


def decode_decrypted_files(
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


def create_table_from_stage(snowflake_conn: str, schema: str, stage: str) -> None:
    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    qualified_table = f"{schema}.{stage}"

    with engine.begin() as tx:
        stmt = f"select $1 as fields from @{qualified_table}"  # nosec

        tx.execute(
            f"create or replace transient table {qualified_table} as {stmt}"  # nosec
        ).fetchall()


def encrypt(fd: IO[bytes]) -> IO[bytes]:
    gpg = _init_gnupg()
    encrypted_message = gpg.encrypt_file(fd, "sts@equifax.com", always_trust=True)
    return BytesIO(encrypted_message.data)


def decrypt(fd: IO[bytes]) -> IO[bytes]:
    gpg = _init_gnupg()
    pass_phrase = Variable.get("equifax_pgp_passphrase", deserialize_json=False)
    decrypted_message = gpg.decrypt_file(fd, always_trust=True, passphrase=pass_phrase)
    return BytesIO(decrypted_message.data)


with DAG(
    dag_id="equifax_commercial_inbox",
    start_date=pendulum.datetime(
        2020, 11, 15, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=slack_dag("slack_data_alerts"),
) as inbox_dag:
    (
        inbox_dag
        << PythonOperator(
            task_id="sync_sshfs_to_s3fs",
            python_callable=sync_sshfs_to_s3fs,
            op_kwargs={
                "aws_conn": "s3_equifax_commercial",
                "sshfs_conn": "ssh_equifax_commercial",
            },
            retry_delay=datetime.timedelta(hours=1),
            retries=3,
            executor_config=EXECUTOR_CONFIG,
        )
        >> PythonOperator(
            task_id="decrypt_received_files",
            python_callable=decrypt_received_files,
            op_kwargs={
                "aws_conn": "s3_equifax_commercial",
            },
            retry_delay=datetime.timedelta(hours=1),
            retries=3,
            executor_config=EXECUTOR_CONFIG,
        )
        >> PythonOperator(
            task_id="convert_to_parquet",
            python_callable=decode_decrypted_files,
            op_kwargs={
                "aws_conn": "s3_equifax_commercial",
            },
            provide_context=True,
            retry_delay=datetime.timedelta(hours=1),
            retries=3,
            executor_config=EXECUTOR_CONFIG,
        )
        >> PythonOperator(
            task_id="create_table_from_stage",
            python_callable=create_table_from_stage,
            op_kwargs={
                "snowflake_conn": "airflow_production",
                "schema": "airflow.production",
                "stage": "equifax_commercial_inbox",
            },
            retry_delay=datetime.timedelta(hours=1),
            retries=3,
            executor_config=EXECUTOR_CONFIG,
        )
    )
