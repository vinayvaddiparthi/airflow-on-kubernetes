import datetime
import json
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, Connection

from dags.utils.failure_callbacks import slack_dag

pytest_plugins = ["helpers_namespace"]
import os
import pytest


@pytest.fixture
def test_dag(mocker):
    """Airflow DAG for testing."""
    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(
            schema="https",
            host="https://hooks.slack.com/services",
            extra=json.dumps({"webhook_token": os.environ.get("SLACK_WEB_HOOK")}),
        ),
    )
    return DAG(
        "test_dag",
        start_date=datetime.datetime(2020, 1, 1),
        schedule_interval=datetime.timedelta(days=1),
        on_failure_callback=slack_dag("conn_id"),
    )


@pytest.helpers.register
def run_task(task, dag):
    """Run an Airflow task."""
    dag.clear()
    task.execute(context={})
