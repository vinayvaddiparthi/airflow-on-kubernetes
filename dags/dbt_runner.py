from airflow import DAG
from datetime import datetime
import pendulum

with DAG(
    "dbt_runner",
    max_active_runs=1,
    schedule_interval=None,
    start_date=pendulum.datetime(
        2020, 4, 21, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:
    dag << DbtOperator(
        task_id="dbt_run",
        pool="snowflake_pool",
        execution_timeout=datetime.timedelta(hours=1),
    ) >> DbtOperator(
        task_id="dbt_snapshot",
        pool="snowflake_pool",
        execution_timeout=datetime.timedelta(hours=1),
        action="snapshot",
    )
