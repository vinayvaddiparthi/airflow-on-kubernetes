from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable

from utils.failure_callbacks import slack_task
from zetatango_import import decrypt_pii_columns
from data.zetatango import (
    qbo_decryption_spec,
)
from custom_operators.rbac_python_operator import RBACPythonOperator


with DAG(
    dag_id="quickbooks_decryption",
    start_date=pendulum.datetime(2022, 2, 23, tz=pendulum.timezone("America/Toronto")),
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

    decryption_role = Variable.get("zetatango_decryption_role")

    decrypt_qbo = RBACPythonOperator(
        task_id="qbo_pii_decryption",
        python_callable=decrypt_pii_columns,
        op_kwargs={
            "snowflake_connection": "snowflake_production",
            "decryption_specs": qbo_decryption_spec,
            "target_schema": f"ZETATANGO.PII_{'PRODUCTION' if is_prod else 'STAGING'}",
        },
        task_iam_role_arn=decryption_role,
        provide_context=True,
    )
