from airflow import DAG
from datetime import timedelta
import pendulum
from dbt_extras.dbt_tasks import dbt_run, dbt_snapshot

with DAG(
    "dbt_runner",
    max_active_runs=1,
    schedule_interval=None,
    start_date=pendulum.datetime(
        2020, 4, 21, tzinfo=pendulum.timezone("America/Toronto")
    ),
) as dag:
    dag << dbt_run >> dbt_snapshot
