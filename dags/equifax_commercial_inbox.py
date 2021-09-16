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
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import datetime
import logging
import pendulum
from typing import Any
from tempfile import NamedTemporaryFile
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
S3_CONN = "s3_dataops"
S3_BUCKET = f"tc-data-airflow-{'production' if Variable.get('environment') == 'production' else 'staging'}"
DIR_PATH = "equifax/commercial"
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


def _download_response_files(
    sftp_conn_id: str,
    s3_conn_id: str,
    dir_path: str,
    s3_bucket: str,
    ti: TaskInstance,
    **_: None,
) -> None:
    """
    As long as we send only one request file, we can safely assume only 2 commercial files (e.g., dv and risk) will be
    available every odd month as Equifax clears the directory after 7 days.
    """
    sftp_hook = SFTPHook(ftp_conn_id=sftp_conn_id)
    files = sftp_hook.list_directory(path="outbox/")
    commercial_files = [
        file for file in files if file.startswith("exthinkingpd.eqxcan.eqxcom")
    ]
    if len(commercial_files) != 2:  # wait until both commercial files are ready
        logging.error(
            "❌ Both commercial files are not yet available to download from the sftp server."
        )
        return
    ti.xcom_push(key="filenames", value=commercial_files)
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    for commercial_file in commercial_files:
        with NamedTemporaryFile(mode="w") as f:
            sftp_hook.retrieve_file(
                remote_full_path=f"outbox/{commercial_file}", local_full_path=f.name
            )
            s3_hook.load_file(
                filename=f.name,
                key=f"{dir_path}/inbox/{commercial_file}",
                bucket_name=s3_bucket,
                replace=True,
            )
    Variable.set("equifax_commercial_response_downloaded", True)
    logging.info("Response files successfully downloaded.")


def _decrypt_response_files(
    s3_conn_id: str, s3_bucket: str, dir_path: str, ti: TaskInstance, **_: None
) -> None:
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    filenames = ti.xcom_pull(task_ids="download_response_files", key="filenames")
    gpg = init_gnupg()
    passphrase = Variable.get("equifax_pgp_passphrase", deserialize_json=False)
    for filename in filenames:
        file = s3_hook.download_file(
            key=f"{dir_path}/inbox/{filename}", bucket_name=s3_bucket
        )
        with open(file, "rb") as reader:
            decrypted_message = gpg.decrypt_file(
                reader, always_trust=True, passphrase=passphrase
            )
        with open(file, "wb") as writer:
            writer.write(decrypted_message.data)
            s3_hook.load_file(
                filename=file,
                key=f"{dir_path}/decrypted/{filename[:-4]}",
                bucket_name=s3_bucket,
                replace=True,
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


def _decode_decrypted_files(
    aws_conn: str, execution_date: Pendulum, run_id: str, **kwargs: Any
) -> None:
    with SuspendAwsEnvVar():
        s3fs = _get_s3fs_from_conn(aws_conn)

        for file in (
            file
            for file in s3fs.listdir("decrypted")
            if f"{file[:-4]}.parquet" not in set(s3fs.listdir("parquet"))
        ):
            with s3fs.open(f"decrypted/{file}", "rb") as decrypted_file, s3fs.open(
                f"parquet/{file[:-4]}.parquet", "wb"
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


check_if_file_downloaded = ShortCircuitOperator(
    task_id="check_if_files_downloaded",
    python_callable=_check_if_file_downloaded,
    do_xcom_push=False,
    dag=dag,
)

download_response_files = PythonOperator(
    task_id="download_response_files",
    python_callable=_download_response_files,
    op_kwargs={
        "sftp_conn_id": SFTP_CONN,
        "s3_conn_id": S3_CONN,
        "dir_path": DIR_PATH,
        "s3_bucket": S3_BUCKET,
    },
    provide_context=True,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

decrypt_response_files = PythonOperator(
    task_id="decrypt_response_files",
    python_callable=_decrypt_response_files,
    op_kwargs={
        "s3_conn_id": S3_CONN,
        "s3_bucket": S3_BUCKET,
        "dir_path": DIR_PATH,
    },
    provide_context=True,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

convert_to_parquet = PythonOperator(
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
    >> download_response_files
    >> decrypt_response_files
    >> convert_to_parquet
    >> create_table_from_stage
)
