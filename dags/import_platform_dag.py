import os
import tempfile
from pathlib import Path

import pendulum
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import subprocess  # nosec


def run_heroku_command(app: str, snowflake_connection: str, snowflake_schema: str):
    os.environ["HEROKU_API_KEY"] = HttpHook.get_connection("heroku_production").password
    snowflake_conn = SnowflakeHook.get_connection(snowflake_connection)

    base_ssh_key_path = Path.home() / ".ssh" / "id_rsa"

    for command in [
        ["ssh-keygen", "-t", "rsa", "-N", "", "-f", base_ssh_key_path],
        ["heroku", "keys:add", f"{base_ssh_key_path}.pub"],
        [
            "heroku",
            "run",
            "-a",
            app,
            "python",
            "extract.py",
            "--snowflake-account",
            "thinkingcapital.ca-central-1.aws",
            "--snowflake-username",
            snowflake_conn.login,
            "--snowflake-password",
            snowflake_conn.password,
            "--snowflake-database",
            "ZETATANGO",
            "--snowflake-schema",
            snowflake_schema,
            "--snowflake-schema",
            snowflake_schema,
        ],
    ]:
        subprocess.run(command, capture_output=True).check_returncode()  # nosec


with DAG(
    dag_id="platform_import_production",
    start_date=pendulum.datetime(
        2020, 3, 20, tzinfo=pendulum.timezone("America/Toronto")
    ),
    schedule_interval="0 0,8-20 * * 1-5",
) as dag:
    for kwargs in [
        {
            "app": "zt-production-elt-core",
            "snowflake_connection": "snowflake_zetatango_production",
            "snowflake_schema": "CORE_PRODUCTION",
        },
        {
            "app": "zt-production-elt-kyc",
            "snowflake_connection": "snowflake_zetatango_production",
            "snowflake_schema": "KYC_PRODUCTION",
        },
        {
            "app": "zt-production-elt-idp",
            "snowflake_connection": "snowflake_zetatango_production",
            "snowflake_schema": "IDP_PRODUCTION",
        },
    ]:
        dag << PythonOperator(
            task_id=f'import_{kwargs["app"]}',
            python_callable=run_heroku_command,
            op_kwargs=kwargs,
        )
