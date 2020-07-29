from dbt_extras.dbt_operator import DbtOperator
from dbt_extras.dbt_action import DbtAction
from datetime import timedelta


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
