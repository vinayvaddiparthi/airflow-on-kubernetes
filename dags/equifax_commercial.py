import datetime
import os
from io import BytesIO

from pathlib import Path
from typing import Tuple, IO, List
from unittest.mock import patch, MagicMock

import pendulum
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from fs.sshfs import SSHFS
from fs.tools import copy_file_data
from fs_s3fs import S3FS

from helpers.suspend_aws_env import SuspendAwsEnvVar

import gnupg

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
    aws_connection = AwsHook.get_connection(aws_conn)

    return S3FS(
        bucket_name=aws_connection.extra_dejson["bucket"],
        region=aws_connection.extra_dejson["region"],
        dir_path=aws_connection.extra_dejson["dir_path"],
    )


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


def sync_sshfs_to_s3fs(aws_conn: str, sshfs_conn: str) -> None:
    with SuspendAwsEnvVar():
        s3fs, sshfs = _get_s3fs_from_conn(aws_conn), _get_sshfs_from_conn(sshfs_conn)

        remote_files = sshfs.listdir("outbox")
        local_files = set(s3fs.listdir("inbox"))  # cast to set for O(1) lookup

        for file in (file for file in remote_files if file not in local_files):
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


def encrypt(fd: IO[bytes]) -> IO[bytes]:
    gpg = _init_gnupg()
    encrypted_message = gpg.encrypt_file(fd, "sts@equifax.com", always_trust=True)
    return BytesIO(encrypted_message.data)


def decrypt(fd: IO[bytes]) -> IO[bytes]:
    gpg = _init_gnupg()
    decrypted_message = gpg.decrypt_file(fd, always_trust=True)
    return BytesIO(decrypted_message.data)


def create_dags() -> Tuple[DAG, DAG]:
    with DAG(
        "equifax_commercial_inbox",
        start_date=pendulum.datetime(
            2020, 11, 15, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval=None,
        catchup=False,
    ) as inbox_dag:
        inbox_dag << PythonOperator(
            task_id="sync_sshfs_to_s3fs",
            python_callable=sync_sshfs_to_s3fs,
            op_kwargs={
                "aws_conn": "s3_equifax_commercial",
                "ssh_conn": "ssh_equifax_commercial",
            },
            retry_delay=datetime.timedelta(hours=1),
            retries=3,
            executor_config=EXECUTOR_CONFIG,
        ) >> PythonOperator(
            task_id="decrypt_received_files",
            python_callable=decrypt_received_files,
            op_kwargs={
                "aws_conn": "s3_equifax_commercial",
            },
            retry_delay=datetime.timedelta(hours=1),
            retries=3,
            executor_config=EXECUTOR_CONFIG,
        )

    with DAG(
        "equifax_commercial_outbox",
        start_date=pendulum.datetime(
            2020, 11, 15, tzinfo=pendulum.timezone("America/Toronto")
        ),
        schedule_interval=None,
        catchup=False,
    ) as outbox_dag:
        outbox_dag << PythonOperator(
            task_id="sync_s3fs_to_sshfs",
            python_callable=sync_s3fs_to_sshfs,
            op_kwargs={
                "aws_conn": "s3_equifax_commercial",
                "ssh_conn": "ssh_equifax_commercial",
            },
            executor_config=EXECUTOR_CONFIG,
        )

    return inbox_dag, outbox_dag


if __name__ == "__main__":
    aws_hook_mock = MagicMock()
    aws_hook_mock.extra_dejson = {
        "bucket": "tc-data-airflow-production",
        "region": "ca-central-1",
        "dir_path": "equifax/commercial",
    }

    ssh_hook_mock = MagicMock()
    ssh_hook_mock.host = os.environ["SSHFS_HOSTNAME"]
    ssh_hook_mock.login = os.environ["SSHFS_USERNAME"]
    ssh_hook_mock.password = os.environ["SSHFS_PASSWORD"]

    with patch(
        "dags.equifax_commercial.AwsHook.get_connection", return_value=aws_hook_mock
    ), patch(
        "dags.equifax_commercial.SSHHook.get_connection", return_value=ssh_hook_mock
    ), patch(
        "dags.equifax_commercial.Variable.get",
        return_value=[
            os.environ.get("EFX_PUBLIC_KEY"),
            os.environ.get("TCAP_PUBLIC_KEY"),
            os.environ.get("TCAP_PRIVATE_KEY"),
        ],
    ):
        sync_s3fs_to_sshfs("aws_conn", "ssh_conn")
        sync_sshfs_to_s3fs("aws_conn", "ssh_conn")
else:
    inbox_dag, outbox_dag = create_dags()
    globals()["inbox_dag"] = inbox_dag
    globals()["outbox_dag"] = outbox_dag