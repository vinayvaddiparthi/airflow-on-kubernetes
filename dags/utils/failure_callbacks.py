from typing import Dict, Callable

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


def slack_ti(conn_id: str) -> Callable:
    def func(context: Dict) -> str:
        return SlackWebhookOperator(
            task_id="slack_fail",
            http_conn_id=conn_id,
            message=f"{context['ti']}",
        ).execute(context=context)

    return func


def slack_dag(conn_id: str) -> Callable:
    def func(context: Dict) -> str:
        return SlackWebhookOperator(
            task_id="slack_fail",
            http_conn_id=conn_id,
            message=f':red_circle: Task failed. {context["task_instance"]}\n'
            f'*Task*: {context["task_instance"].task_id}\n'
            f'*Dag*: {context["task_instance"].dag_id}\n'
            f'*Execution Time*: {context["execution_date"]}\n'
            f'*Log Url*: {context["task_instance"].log_url}',
        ).execute(context=context)

    return func
