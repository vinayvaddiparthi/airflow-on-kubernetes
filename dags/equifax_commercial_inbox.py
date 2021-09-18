"""
### Description
This workflow processes the commercial response file from Equifax. Once the response file is downloaded, the encrypted
response file will be processed and uploaded to Snowflake.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import datetime
import logging
import pendulum
from tempfile import NamedTemporaryFile
from fs_s3fs import S3FS
import pyarrow.csv as csv
import pyarrow.parquet as pq
from pyarrow._csv import ReadOptions
from pyarrow.lib import ArrowInvalid, array

from helpers.suspend_aws_env import SuspendAwsEnvVar
from utils.failure_callbacks import slack_dag, sensor_timeout
from utils.gpg import init_gnupg
from utils.equifax_helpers import get_import_month

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
    template_searchpath="include/sql",
)
dag.doc_md = __doc__

IS_PROD = Variable.get(key="environment") == "production"
SFTP_CONN = "equifax_sftp"
SNOWFLAKE_CONN = "airflow_production"
S3_CONN = "s3_dataops"
S3_BUCKET = f"tc-data-airflow-{'production' if IS_PROD else 'staging'}"
DIR_PATH = "equifax/commercial"
DV_FILENAME = (
    "{{ ti.xcom_pull(task_ids='download_response_files', key='dv_filename') }}"
)
RISK_FILENAME = (
    "{{ ti.xcom_pull(task_ids='download_response_files', key='risk_filename') }}"
)
DV_STAGE_NAME = f"equifax.{'public' if IS_PROD else 'test'}.equifax_comm_stage"
RISK_STAGE_NAME = f"equifax.{'public' if IS_PROD else 'test'}.equifax_tcap_stage"
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
    return not Variable.get(
        "equifax_commercial_response_downloaded", deserialize_json=True
    )


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
    for file in commercial_files:
        if "dv" in file:
            ti.xcom_push(key="dv_filename", value=file[:-8])
        else:
            ti.xcom_push(key="risk_filename", value=file[:-8])
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
    Variable.set("equifax_commercial_response_downloaded", True, serialize_json=True)
    logging.info("Response files successfully downloaded.")


def _decrypt_response_files(
    s3_conn_id: str, s3_bucket: str, dir_path: str, ti: TaskInstance, **_: None
) -> None:
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    dv_filename = ti.xcom_pull(task_ids="download_response_files", key="dv_filename")
    risk_filename = ti.xcom_pull(task_ids="download_response_files", key="dv_filename")
    gpg = init_gnupg()
    passphrase = Variable.get("equifax_pgp_passphrase", deserialize_json=False)
    for filename in [dv_filename, risk_filename]:
        file = s3_hook.download_file(
            key=f"{dir_path}/inbox/{filename}.csv.pgp", bucket_name=s3_bucket
        )
        with open(file, "rb") as reader:
            decrypted_message = gpg.decrypt_file(
                reader, always_trust=True, passphrase=passphrase
            )
        with open(file, "wb") as writer:
            writer.write(decrypted_message.data)
            s3_hook.load_file(
                filename=file,
                key=f"{dir_path}/decrypted/{filename}.csv",
                bucket_name=s3_bucket,
                replace=True,
            )


def _convert_to_parquet(
    s3_conn_id: str,
    bucket_name: str,
    snowflake_conn_id: str,
    stage_names: dict,
    ds_nodash: str,
    ti: TaskInstance,
    **_: None,
) -> None:
    snowflake_engine = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id
    ).get_sqlalchemy_engine()
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    for filename in [
        ti.xcom_pull(task_ids="download_response_files", key="dv_filename"),
        ti.xcom_pull(task_ids="download_response_files", key="risk_filename"),
    ]:
        file = s3_hook.download_file(
            key=f"{DIR_PATH}/decrypted/{filename}.csv", bucket_name=bucket_name
        )
        with open(file, "rb") as reader:
            try:
                table_ = csv.read_csv(
                    reader,
                    read_options=ReadOptions(block_size=8388608),
                )
                # add imported_file_name and import_month columns
                table_ = table_.append_column(
                    field_="imported_file_name",
                    column=array([f"{filename}.parquet"] * len(table_)),
                )
                table_ = table_.append_column(
                    field_="import_month",
                    column=array([get_import_month(ds_nodash)] * len(table_)),
                )
                if table_.num_rows == 0:
                    logging.warning(f"📝️ Skipping empty file {reader.name}")
                    continue
            except ArrowInvalid as exc:
                logging.error(f"❌ Failed to read file {reader.name}: {exc}")
        with open(file, "wb") as writer:
            pq.write_table(table=table_, where=writer)
            logging.info(f"📝️ Converted file {filename}.csv")
            s3_hook.load_file(
                filename=file,
                key=f"{DIR_PATH}/parquet/{filename}.parquet",
                bucket_name=bucket_name,
                replace=True,
            )
        with snowflake_engine.begin() as tx:
            if "dv" in filename:
                stage_name = stage_names["dv_stage_name"]
            else:
                stage_name = stage_names["risk_stage_name"]
            tx.execute(
                f"create or replace temporary stage {stage_name} file_format=(type=parquet)"
            )
            tx.execute(f"put file://{file} @{stage_name}").fetchall()


def _create_table_from_stage(snowflake_conn: str, schema: str, stage: str) -> None:
    engine = SnowflakeHook(snowflake_conn).get_sqlalchemy_engine()
    qualified_table = f"{schema}.{stage}"

    with engine.begin() as tx:
        stmt = f"select $1 as fields from @{qualified_table}"  # nosec

        tx.execute(
            f"create or replace transient table {qualified_table} as {stmt}"  # nosec
        ).fetchall()


check_if_files_downloaded = ShortCircuitOperator(
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
    python_callable=_convert_to_parquet,
    op_kwargs={
        "snowflake_conn_id": SNOWFLAKE_CONN,
        "s3_conn_id": S3_CONN,
        "bucket_name": S3_BUCKET,
        "stage_names": {
            "dv_stage_name": DV_STAGE_NAME,
            "risk_stage_name": RISK_STAGE_NAME,
        },
    },
    provide_context=True,
    retry_delay=datetime.timedelta(hours=1),
    retries=3,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

# is_dv_parquet_available = S3KeySensor(
#     task_id="is_dv_parquet_available",
#     bucket_name=S3_BUCKET,
#     bucket_key=f"{DIR_PATH}/parquet/{DV_FILENAME}.parquet",
#     aws_conn_id=S3_CONN,
#     poke_interval=5,
#     timeout=20,
#     on_failure_callback=sensor_timeout,
#     dag=dag,
# )
#
# is_risk_parquet_available = S3KeySensor(
#     task_id="is_risk_parquet_available",
#     bucket_name=S3_BUCKET,
#     bucket_key=f"{DIR_PATH}/parquet/{RISK_FILENAME}.parquet",
#     aws_conn_id=S3_CONN,
#     poke_interval=5,
#     timeout=20,
#     on_failure_callback=sensor_timeout,
#     dag=dag,
# )

# create_s3_stage = SnowflakeOperator(
#     task_id="create_s3_stage",
#     sql="snowflake/common/create_s3_stage.sql",
#     params={
#         "stage_name": "equifax_comm_stage",
#         "s3_key": f"s3://{S3_BUCKET}/{DIR_PATH}/parquet/{DV_FILENAME}.pgp",
#         "aws_key_id": "",
#         "aws_secret_key": "",
#         "file_format": "parquet",
#     },
#     schema="test",
#     database="equifax",
#     snowflake_conn_id=SNOWFLAKE_CONN,
#     dag=dag,
# )

create_stage_table_dv = SnowflakeOperator(
    task_id=f"create_stage_table_dv",
    sql="equifax/create_staging_table_dv.sql",
    params={"table_name": "equifax_comm_staging"},
    schema=f"{'public' if IS_PROD else 'test'}",
    database="equifax",
    snowflake_conn_id=SNOWFLAKE_CONN,
    dag=dag,
)

create_stage_table_risk = SnowflakeOperator(
    task_id=f"create_stage_table_risk",
    sql="equifax/create_table.sql",
    params={"table_name": "equifax_tcap_staging", "stage_name": "equifax_tcap_stage"},
    schema=f"{'public' if IS_PROD else 'test'}",
    database="equifax",
    snowflake_conn_id=SNOWFLAKE_CONN,
    dag=dag,
)

truncate_stage_table = SnowflakeOperator(
    task_id="truncate_stage_table",
    sql="snowflake/common/truncate_table.sql",
    params={"table_name": "equifax_comm_staging"},
    schema=f"{'public' if IS_PROD else 'test'}",
    database="equifax",
    snowflake_conn_id=SNOWFLAKE_CONN,
    dag=dag,
)

# copy_from_s3_to_snowflake = S3ToSnowflakeOperator(
#     task_id="copy_from_s3_to_snowflake",
#     s3_keys=[
#         f"s3://{S3_BUCKET}/{DIR_PATH}/parquet/exthinkingpd.eqxcan.eqxcom.efx_scores_dv_20210907_t1.parquet",
#     ],
#     stage="equifax_comm_stage",
#     file_format="(type=parquet)",
#     table="equifax_comm_staging",
#     schema="test",
#     database="equifax",
#     # warehouse="etl",
#     snowflake_conn_id=SNOWFLAKE_CONN,
#     dag=dag,
# )

create_table_from_stage = PythonOperator(
    task_id="create_table_from_stage",
    python_callable=_create_table_from_stage,
    op_kwargs={
        "snowflake_conn": SNOWFLAKE_CONN,
        "schema": "airflow.production",
        "stage": "equifax_commercial_inbox",
    },
    retry_delay=datetime.timedelta(hours=1),
    retries=3,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

(
    check_if_files_downloaded
    >> download_response_files
    >> decrypt_response_files
    >> convert_to_parquet
    >> [create_stage_table_dv, create_stage_table_risk]
)
