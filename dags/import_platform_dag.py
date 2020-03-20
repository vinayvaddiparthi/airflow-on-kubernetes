import os

import pendulum
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import subprocess


def run_heroku_command(app: str, snowflake_connection, snowflake_schema):
    os.environ["HEROKU_API_KEY"] = HttpHook("heroku_production").get_conn().auth[1]

    subprocess.run(["ssh-keygen", "-t", "rsa", "-N", "", "-f", "id_rsa"])
    subprocess.run(["ssh-add", "id_rsa"])
    subprocess.run(["heroku", "keys:add", "id_rsa.pub"])
    subprocess.run(
        [
            "heroku",
            "run",
            "-a",
            app,
            "-e",
            f"SNOWFLAKE_PASSWORD={snowflake_connection.password}",
            "python",
            "extract.py",
            "--snowflake-account",
            "thinkingcapital.ca-central-1.aws",
            "--snowflake-username",
            snowflake_connection.login,
            "--snowflake-password",
            "$SNOWFLAKE_PASSWORD",
            "--snowflake-database",
            "ZETATANGO",
            "--snowflake-schema",
            snowflake_schema,
        ]
    )
    subprocess.run(["heroku", "keys:clear"])


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
