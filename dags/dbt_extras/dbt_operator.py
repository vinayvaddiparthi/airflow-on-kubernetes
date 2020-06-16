from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults
from dbt_extras.dbt_action import DbtAction


class DbtOperator(BashOperator):
    """
    Pass one dbt action for the DbtOperator to execute. Default action is `dbt run`
    Environments can be passed by a json object, or default to below, using conn `airflow
    Hide credentials in the operator, leave nothing in runner dag
    """

    # snowflake_hook = BaseHook.get_connection("airflow_production")
    # dbt_hook = BaseHook.get_connection("dbt_refresh")
    gitlab_user = "dbt_hook.login"
    gitlab_token = "dbt_hook.password"

    @apply_defaults
    def __init__(
        self,
        action=DbtAction.run,
        profiles_args=".",
        target_args="prod",
        env={
            "SNOWFLAKE_ACCOUNT": "snowflake_hook.host",
            "SNOWFLAKE_USERNAME": "snowflake_hook.login",
            "SNOWFLAKE_PASSWORD": "snowflake_hook.password",
            "SNOWFLAKE_DATABASE": "db",
            "SNOWFLAKE_SCHEMA": "schema",
            "SNOWFLAKE_ROLE": "role",
            "SNOWFLAKE_WAREHOUSE": "warehouse",
            "ZETATANGO_ENV": "PRODUCTION",
            "GITLAB_USER": gitlab_user,
            "GITLAB_TOKEN": gitlab_token,
        },
        *args,
        **kwargs,
    ):
        profiles = f"--profiles-dir {profiles_args}"
        target = f"--target {target_args}"
        command = " ".join(["dbt", action.name, profiles, target])
        print(f"Execute command: {command}")

        super(DbtOperator, self).__init__(
            bash_command="git clone https://${GITLAB_USER}:${GITLAB_TOKEN}@gitlab.com/tc-data/curated-data-warehouse.git"
            "&& cd curated-data-warehouse"
            f"&& {command}",
            env=env,
            *args,
            **kwargs,
        )
