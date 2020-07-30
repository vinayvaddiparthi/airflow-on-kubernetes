from airflow import DAG
from datetime import timedelta
import pendulum
from dbt_extras.dbt_operator import DbtOperator
from dbt_extras.dbt_action import DbtAction


dbt_run = DbtOperator(
    task_id="dbt_run",
    execution_timeout=timedelta(hours=1),
    action=DbtAction.run,
)

dbt_snapshot = DbtOperator(
    task_id="dbt_snapshot",
    execution_timeout=timedelta(hours=1),
    action=DbtAction.snapshot,
)

with DAG(
    "dbt_runner",
    max_active_runs=1,
    schedule_interval=None,
    start_date=pendulum.datetime(
        2020, 4, 21, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:
    dag << dbt_run >> dbt_snapshot
