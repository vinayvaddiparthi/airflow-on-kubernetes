# Refresh oauth token every 4 hours, N/A use airflow production and get the conn
# Make individual runner for snapshot, run, compile, doc, debug
# Hide creds in the operator, leave nothing in runner dag
from airflow.utils.decorators import apply_defaults
from airflow.operators.bash_operator import BashOperator


class DbtOperator(BashOperator):
    dbt_hook = BaseHook.get_connection("dbt_refresh")
    gl_user = json.loads(dbt_hook.extra)["user"]
    gl_token = json.loads(dbt_hook.extra)["token"]

    @apply_defaults
    def __init__(
        self,
        action="run",
        ext="--profiles-dir . --target prod",
        env={
            "SNOWFLAKE_ACCOUNT": dbt_hook.host,
            "SNOWFLAKE_USERNAME": dbt_hook.login,
            "SNOWFLAKE_PASSWORD": dbt_hook.password,
            "SNOWFLAKE_DATABASE": "ANALYTICS_PRODUCTION",
            "SNOWFLAKE_SCHEMA": "DBT_ARIO",
            "SNOWFLAKE_ROLE": "DBT_PRODUCTION",
            "SNOWFLAKE_WAREHOUSE": "ETL",
            "ZETATANGO_ENV": "PRODUCTION",
            "gl_user": gl_user,
            "gl_token": gl_token,
        },
        *args,
        **kwargs,
    ):

        command = (" ".join(["dbt", action, ext]),)
        print(f"command: {command}")

        super(DbtOperator, self).__init__(
            bash_command="git clone https://${gl_user}:${gl_token}@gitlab.com/tc-data/curated-data-warehouse.git"
            "&& cd curated-data-warehouse"
            f"&& {command}",
            env=env,
            *args,
            **kwargs,
        )
