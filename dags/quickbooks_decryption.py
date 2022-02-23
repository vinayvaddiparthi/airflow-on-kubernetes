from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from utils.failure_callbacks import slack_task
from zetatango_import import decrypt_pii_columns
from data.zetatango import (
    decryption_executor_config,
    qbo_decryption_spec,
)


with DAG(
    dag_id="quickbooks_decryption",
    start_date=pendulum.datetime(
        2022, 2, 23, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="0 2 * * *",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack_task("slack_data_alerts"),
    },
    catchup=False,
    max_active_runs=1,
) as dag:

    is_prod = Variable.get(key="environment") == "production"

    decrypt_qbo_prod = PythonOperator(
        task_id="qbo__pii_decryption",
        python_callable=decrypt_pii_columns,
        op_kwargs={
            "snowflake_connection": "snowflake_production",
            "decryption_specs": qbo_decryption_spec,
            "target_schema": f"ZETATANGO.PII_{'PRODUCTION' if is_prod else 'STAGING'}",
        },
        executor_config=decryption_executor_config,
    )

    dag << decrypt_qbo_prod
