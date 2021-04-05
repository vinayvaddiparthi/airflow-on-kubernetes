import json
import pytest
import os
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection


def test_slack_failure(test_dag, mocker):
    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(
            schema="https",
            host="https://hooks.slack.com/services",
            extra=json.dumps({"webhook_token": os.environ.get("SLACK_WEB_HOOK")}),
        ),
    )
    test = SlackWebhookOperator(
        task_id="slack_fail",
        http_conn_id="conn_id",
        message=f"DagRun failed: Test from local",
    )

    pytest.helpers.run_task(task=test, dag=test_dag)
