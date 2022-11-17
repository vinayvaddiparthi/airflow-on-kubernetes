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
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import logging
import pendulum
from tempfile import NamedTemporaryFile
import pyarrow.csv as csv
import pyarrow.parquet as pq
from pyarrow._csv import ReadOptions
from pyarrow.lib import ArrowInvalid, array
from typing import List
from utils.common_utils import get_utc_timestamp
from datetime import timedelta

from utils.failure_callbacks import slack_task, slack_dag_success
from dbt_extras.dbt_operator import DbtOperator
from dbt_extras.dbt_action import DbtAction
from utils.gpg import init_gnupg
from utils.equifax_helpers import get_import_month

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(
        2020, 11, 15, tz=pendulum.timezone("America/Toronto")
    ),
    "retries": 0,
    "catchup": False,
    "on_failure_callback": slack_task("slack_data_alerts"),
    "tags": ["equifax"],
    "description": "A workflow to download and process the commercial batch response file from Equifax",
    "default_view": "graph",
}

dag = DAG(
    dag_id="equifax_commercial_inbox",
    schedule_interval="0 12 * * *",
    default_args=default_args,
    template_searchpath="dags/sql",
    catchup=False,
)
dag.doc_md = __doc__

IS_PROD = Variable.get(key="environment") == "production"
SFTP_CONN = "equifax_sftp"
SNOWFLAKE_CONN = "snowflake_production"
S3_CONN = "s3_dataops"
S3_BUCKET = f"tc-data-airflow-{'production' if IS_PROD else 'staging'}"
DIR_PATH = "equifax/commercial"
RISK_STAGE_NAME = f"equifax.{'public' if IS_PROD else 'test'}.equifax_comm_stage"
DV_STAGE_NAME = f"equifax.{'public' if IS_PROD else 'test'}.equifax_tcap_stage"
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


def _fetch_files_from_sftp(sftp_conn_id: str) -> List[str]:

    sftp_hook = SFTPHook(ftp_conn_id=sftp_conn_id)
    files = sftp_hook.list_directory(path="outbox/")
    commercial_files = [
        file for file in files if file.startswith("exthinkingpd.eqxcan.eqxcom")
    ]

    return commercial_files


def _check_if_response_available(
    sftp_conn_id: str,
) -> bool:

    commercial_file_list = _fetch_files_from_sftp(sftp_conn_id)

    if len(commercial_file_list) != 2:  # wait until both commercial files are ready
        logging.error(
            "❌ Both commercial files are not yet available to download from the sftp server."
        )
        return False
    return True


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
    commercial_files = _fetch_files_from_sftp(sftp_conn_id)

    for file in commercial_files:
        if "risk" in file:
            ti.xcom_push(key="risk_filename", value=file[:-8])
        else:
            ti.xcom_push(key="dv_filename", value=file[:-8])
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
    risk_filename = ti.xcom_pull(
        task_ids="download_response_files", key="risk_filename"
    )
    dv_filename = ti.xcom_pull(task_ids="download_response_files", key="dv_filename")
    gpg = init_gnupg()
    passphrase = Variable.get("equifax_pgp_passphrase", deserialize_json=False)
    for filename in [risk_filename, dv_filename]:
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
    next_ds_nodash: str,
    ti: TaskInstance,
    **_: None,
) -> None:
    snowflake_engine = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id
    ).get_sqlalchemy_engine()
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    for filename in [
        ti.xcom_pull(task_ids="download_response_files", key="risk_filename"),
        ti.xcom_pull(task_ids="download_response_files", key="dv_filename"),
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
                    column=array([get_import_month(next_ds_nodash)] * len(table_)),
                )
                table_ = table_.append_column(
                    field_="import_ts",
                    column=array([f"{get_utc_timestamp()}"] * len(table_)),
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
            if "risk" in filename:
                stage_name = stage_names["risk_stage_name"]
            else:
                stage_name = stage_names["dv_stage_name"]
            tx.execute(
                f"create or replace stage {stage_name} file_format=(type=parquet)"
            )
            tx.execute(f"put file://{file} @{stage_name}").fetchall()


check_if_files_downloaded = ShortCircuitOperator(
    task_id="check_if_files_downloaded",
    python_callable=_check_if_file_downloaded,
    do_xcom_push=False,
    dag=dag,
)


check_if_response_available = ShortCircuitOperator(
    task_id="check_if_response_available",
    python_callable=_check_if_response_available,
    op_kwargs={
        "sftp_conn_id": SFTP_CONN,
    },
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
            "risk_stage_name": RISK_STAGE_NAME,
            "dv_stage_name": DV_STAGE_NAME,
        },
    },
    provide_context=True,
    executor_config=EXECUTOR_CONFIG,
    dag=dag,
)

(
    check_if_files_downloaded
    >> check_if_response_available
    >> download_response_files
    >> decrypt_response_files
    >> convert_to_parquet
)

for file, table in [("risk", "comm"), ("dv", "tcap")]:
    create_staging_table = SnowflakeOperator(
        task_id=f"create_staging_table_{file}",
        sql=f"equifax/staging/create_staging_table_{file}.sql",
        params={"table_name": f"equifax_{table}_staging"},
        schema=f"{'public' if IS_PROD else 'test'}",
        database="equifax",
        snowflake_conn_id=SNOWFLAKE_CONN,
        executor_config=EXECUTOR_CONFIG,
        dag=dag,
    )

    load_from_stage = SnowflakeOperator(
        task_id=f"load_from_stage_{file}",
        sql=f"equifax/load/load_from_stage_{file}.sql",
        params={
            "table_name": f"equifax_{table}_staging",
            "stage_name": f"equifax_{table}_stage",
        },
        schema=f"{'public' if IS_PROD else 'test'}",
        database="equifax",
        snowflake_conn_id=SNOWFLAKE_CONN,
        executor_config=EXECUTOR_CONFIG,
        dag=dag,
    )

    insert_from_staging_table = SnowflakeOperator(
        task_id=f"insert_from_staging_table_{file}",
        sql="snowflake/common/insert_into_table.sql",
        params={
            "table_name": f"equifax_{table}",
            "source_table_name": f"equifax_{table}_staging",
        },
        schema=f"{'public' if IS_PROD else 'test'}",
        database="equifax",
        snowflake_conn_id=SNOWFLAKE_CONN,
        executor_config=EXECUTOR_CONFIG,
        dag=dag,
    )
    refresh_dbt_model = DbtOperator(
        task_id="dbt_run_last_commercial_bureau_pull",
        execution_timeout=timedelta(hours=1),
        action=DbtAction.run,
        models=("last_commercial_bureau_pull"),
        on_success_callback=slack_dag_success("slack_success_alerts_equifax"),
        dag=dag,
    )

    (
        convert_to_parquet
        >> create_staging_table
        >> load_from_stage
        >> insert_from_staging_table
        >> refresh_dbt_model
    )
