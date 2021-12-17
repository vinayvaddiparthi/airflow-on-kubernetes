import pendulum
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook


def check_sftp_connection(ftp_conn_id: str) -> None:
    hook = SFTPHook(ftp_conn_id=ftp_conn_id)
    inbox_files = hook.list_directory(path="inbox/")
    outbox_files = hook.list_directory(path="outbox/")
    logging.info(f"inbox_files: {inbox_files}")
    logging.info(f"outbox_files: {outbox_files}")


with DAG(
    dag_id="equifax_sftp_server_connection_test",
    default_args={"owner": "airflow"},
    start_date=pendulum.datetime(
        2021, 12, 17, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval=None,
    description="test connection to equifax sftp server for batch bureau pull",
    tags=["equifax"],
) as dag:

    check_sftp_connection = PythonOperator(
        task_id="check_sftp_connection",
        python_callable=check_sftp_connection,
        op_kwargs={"ftp_conn_id": "equifax_sftp"},
    )
