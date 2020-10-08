from typing import Dict, Callable

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


def slack_ti(conn_id: str) -> Callable:
    def func(context: Dict) -> str:
        return SlackWebhookOperator(
            task_id="slack_fail",
            http_conn_id=conn_id,
            message=f"<!here> {context['ti']}",
        ).execute(context=context)

    return func
