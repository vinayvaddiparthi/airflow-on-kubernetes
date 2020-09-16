import logging

from enum import Enum
from typing import Optional, Dict, Any

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults

from dbt_extras.dbt_action import DbtAction


class DbtOperator(BashOperator):
    """
    Pass one dbt action for the DbtOperator to execute. Default action is `dbt run`
    Environments can be passed by a json object, or default to below, using conn `airflow
    Hide credentials in the operator, leave nothing in runner dag
    """

    @apply_defaults
    def __init__(
        self,
        action: Enum = DbtAction.run,
        profiles_args: str = ".",
        target_args: str = "prod",
        env: Optional[Dict] = None,
        models: Optional[str] = None,
        snowflake_conn: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ):
        dbt_hook = BaseHook.get_connection("dbt_refresh")
        gitlab_user = dbt_hook.login
        gitlab_token = dbt_hook.password

        snowflake_hook = SnowflakeHook.get_connection(snowflake_conn or "snowflake_dbt")

        env = (
            {
                "SNOWFLAKE_USERNAME": snowflake_hook.login,
                "SNOWFLAKE_PASSWORD": snowflake_hook.password,
                "SNOWFLAKE_ACCOUNT": snowflake_hook.extra_dejson.get("account"),
                "SNOWFLAKE_DATABASE": snowflake_hook.extra_dejson.get("database"),
                "SNOWFLAKE_SCHEMA": snowflake_hook.extra_dejson.get("schema"),
                "SNOWFLAKE_ROLE": snowflake_hook.extra_dejson.get("role"),
                "SNOWFLAKE_WAREHOUSE": snowflake_hook.extra_dejson.get("warehouse"),
                "ZETATANGO_ENV": "PRODUCTION",
                "GITLAB_USER": gitlab_user,
                "GITLAB_TOKEN": gitlab_token,
            }
            if not env
            else env
        )

        model_argument = f"--models {models}" if models else ""

        profiles = f"--profiles-dir {profiles_args}"
        target = f"--target {target_args}"
        deps = " ".join(["dbt", DbtAction.deps.name, profiles, target])
        command = " ".join(["dbt", action.name, profiles, target, model_argument])

        logging.info(f"Execute command: {command}")

        super(DbtOperator, self).__init__(
            bash_command="git clone https://${GITLAB_USER}:${GITLAB_TOKEN}@gitlab.com/tc-data/curated-data-warehouse.git"
            " && cd curated-data-warehouse"
            " && git pull origin master"
            f" && {deps}"
            f" && {command}",
            env=env,
            *args,
            **kwargs,
        )
