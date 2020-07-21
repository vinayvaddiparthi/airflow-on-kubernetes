from airflow import DAG
from datetime import timedelta
import pendulum
from dbt_extras.dbt_operator import DbtOperator
from dbt_extras.dbt_action import DbtAction


dbt_run = DbtOperator(
    task_id="dbt_run",
    pool="snowflake_pool",
    execution_timeout=timedelta(hours=1),
    action=DbtAction.run,
)

dbt_snapshot = DbtOperator(
    task_id="dbt_snapshot",
    pool="snowflake_pool",
    execution_timeout=timedelta(hours=1),
    action=DbtAction.snapshot,
)

dag = DAG(
    "dbt_runner",
    max_active_runs=1,
    schedule_interval=None,
    start_date=pendulum.datetime(
        2020, 4, 21, tzinfo=pendulum.timezone("America/Toronto")
    ),
)

dag << dbt_run >> dbt_snapshot
