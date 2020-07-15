from typing import Dict

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


def slack_on_fail(context: Dict) -> str:
    return SlackWebhookOperator(
        task_id="slack_fail",
        http_conn_id="slack_tc_data_channel",
        message=f"<!here> {context['ti']}",
    ).execute(context=context)
