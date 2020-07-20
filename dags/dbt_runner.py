from airflow import DAG
from datetime import timedelta
import pendulum
from dbt_extras.dbt_operator import DbtOperator
from dbt_extras.dbt_action import DbtAction
from airflow.operators.sensors import ExternalTaskSensor

with DAG(
    "dbt_runner",
    max_active_runs=1,
    schedule_interval=None,
    start_date=pendulum.datetime(
        2020, 4, 21, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:
    dag << ExternalTaskSensor(
        task_id="dbt_check",
        external_dag_id="zetatango_import",
        execution_delta=timedelta(hours=1),
        timeout=300,
    ) >> DbtOperator(
        task_id="dbt_run",
        pool="snowflake_pool",
        execution_timeout=timedelta(hours=1),
        action=DbtAction.run,
    ) >> DbtOperator(
        task_id="dbt_snapshot",
        pool="snowflake_pool",
        execution_timeout=timedelta(hours=1),
        action=DbtAction.snapshot,
    )
