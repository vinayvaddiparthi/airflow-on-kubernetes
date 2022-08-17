from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.exceptions import AirflowSensorTimeout

from typing import Dict, Callable
import logging


def slack_dag(conn_id: str) -> Callable:
    def func(context: Dict) -> str:
        return SlackWebhookHook(
            http_conn_id=conn_id,
            message=f':red_circle: DAG failed. {context["task_instance"].dag_id}\n'
            f'*Execution Time*: {context["execution_date"]}',
        ).execute()

    return func


def slack_dag_success(conn_id: str) -> Callable:
    def func(context: Dict) -> str:
        return SlackWebhookHook(
            channel="#monthly_bureau_pulls",
            http_conn_id=conn_id,
            message=f"✅ DAG run successful. Equifax Batch Data has been uploaded\n"
            f'*DAG ID*:{context["task_instance"].dag_id}\n'
            f'*Execution Time*: {context["execution_date"]}',
        ).execute()

    return func


def slack_task(conn_id: str) -> Callable:
    def func(context: Dict) -> str:
        return SlackWebhookHook(
            http_conn_id=conn_id,
            message=f':red_circle: Task failed. {context["task_instance"]}\n'
            f'*Task*: {context["task_instance"].task_id}\n'
            f'*Dag*: {context["task_instance"].dag_id}\n'
            f'*Execution Time*: {context["execution_date"]}\n'
            f'*Log Url*: {context["task_instance"].log_url}',
        ).execute()

    return func


def sensor_timeout(context: Dict) -> None:
    if isinstance(context["exception"], AirflowSensorTimeout):
        logging.info(context["task_instance"].log_url)
        logging.info("Sensor timed out")
