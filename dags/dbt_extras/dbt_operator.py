# Pass one dbt action or a list of actions for the DbtOperator to execute. Default action is `dbt run`
# Environments can be passed by a json object, or default to below, using conn `airflow
# Hide creds in the operator, leave nothing in runner dag
from airflow.utils.decorators import apply_defaults
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook


class DbtOperator(BashOperator):
    snowflake_hook = BaseHook.get_connection("airflow_production")
    dbt_hook = BaseHook.get_connection("dbt_refresh")
    gitlab_user = json.loads(dbt_hook.extra)["user"]
    gitlab_token = json.loads(dbt_hook.extra)["token"]

    @apply_defaults
    def __init__(
        self,
        action="run",
        ext="--profiles-dir . --target prod",
        env={
            "SNOWFLAKE_ACCOUNT": snowflake_hook.host,
            "SNOWFLAKE_USERNAME": snowflake_hook.login,
            "SNOWFLAKE_PASSWORD": snowflake_hook.password,
            "SNOWFLAKE_DATABASE": snowflake_hook.extra_dejson.get("database", None),
            "SNOWFLAKE_SCHEMA": snowflake_hook.extra_dejson.get("schema", None),
            "SNOWFLAKE_ROLE": snowflake_hook.extra_dejson.get("role", None),
            "SNOWFLAKE_WAREHOUSE": snowflake_hook.extra_dejson.get("warehouse", None),
            "ZETATANGO_ENV": "PRODUCTION",
            "gitlab_user": gitlab_user,
            "gitlab_token": gitlab_token,
        },
        *args,
        **kwargs,
    ):
        command = (" ".join(["dbt", action, ext]),)
        print(f"command: {command}")

        super(DbtOperator, self).__init__(
            bash_command="git clone https://${gitlab_user}:${gitlab_token}@gitlab.com/tc-data/curated-data-warehouse.git"
            "&& cd curated-data-warehouse"
            f"&& {command}",
            env=env,
            *args,
            **kwargs,
        )
