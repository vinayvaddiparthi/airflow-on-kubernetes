import os

import pendulum
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import subprocess  # nosec


def run_heroku_command(app: str, snowflake_connection: str, snowflake_schema: str):
    os.environ["HEROKU_API_KEY"] = HttpHook.get_connection("heroku_production").password
    snowflake_conn = SnowflakeHook.get_connection(snowflake_connection)

    try:
        [
            subprocess.run(command)  # nosec
            for command in [
                ["ssh-keygen", "-t", "rsa", "-N", "", "-f", "id_rsa"],
                ["ssh-add", "id_rsa"],
                ["heroku", "keys:add", "id_rsa.pub"],
                [
                    "heroku",
                    "run",
                    "-a",
                    app,
                    "bash",
                    "-c",
                    f"--snowflake-account thinkingcapital.ca-central-1.aws "
                    f"--snowflake-username {snowflake_conn.login} "
                    f"--snowflake-password {snowflake_conn.password} "
                    f"--snowflake-database ZETATANGO "
                    f"--snowflake-schema {snowflake_schema}",
                ],
            ]
        ]
    finally:
        subprocess.run(["heroku", "keys:clear"])  # nosec


with DAG(
    dag_id="platform_import_production",
    start_date=pendulum.datetime(
        2020, 3, 20, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="@hourly",
) as dag:
    dag << PythonOperator(
        task_id="import_core_production",
        python_callable=run_heroku_command,
        op_kwargs={
            "app": "zt-production-elt-core",
            "snowflake_connection": "snowflake_zetatango_production",
            "snowflake_schema": "CORE_PRODUCTION",
        },
    )
    dag << PythonOperator(
        task_id="import_kyc_production",
        python_callable=run_heroku_command,
        op_kwargs={
            "app": "zt-production-elt-kyc",
            "snowflake_connection": "snowflake_zetatango_production",
            "snowflake_schema": "KYC_PRODUCTION",
        },
    )
    dag << PythonOperator(
        task_id="import_idp_production",
        python_callable=run_heroku_command,
        op_kwargs={
            "app": "zt-production-elt-idp",
            "snowflake_connection": "snowflake_zetatango_production",
            "snowflake_schema": "IDP_PRODUCTION",
        },
    )
