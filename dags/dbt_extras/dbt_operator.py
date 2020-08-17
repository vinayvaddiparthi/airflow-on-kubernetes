import logging

from enum import Enum
from typing import Optional, Dict, Any

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

    dbt_hook = BaseHook.get_connection("dbt_refresh")
    gitlab_user = dbt_hook.login
    gitlab_token = dbt_hook.password

    snowflake_hook = BaseHook.get_connection("airflow_production")

    @apply_defaults
    def __init__(
        self,
        action: Enum = DbtAction.run,
        profiles_args: str = ".",
        target_args: str = "prod",
        env: Optional[Dict] = None,
        models: str = "",
        *args: Any,
        **kwargs: Any,
    ):
        if not env:
            env = {
                "SNOWFLAKE_ACCOUNT": self.__class__.snowflake_hook.host,
                "SNOWFLAKE_USERNAME": self.__class__.snowflake_hook.login,
                "SNOWFLAKE_PASSWORD": self.__class__.snowflake_hook.password,
                "SNOWFLAKE_DATABASE": self.__class__.dbt_hook.extra_dejson.get(
                    "database", None
                ),
                "SNOWFLAKE_SCHEMA": self.__class__.dbt_hook.extra_dejson.get(
                    "schema", None
                ),
                "SNOWFLAKE_ROLE": self.__class__.dbt_hook.extra_dejson.get(
                    "role", None
                ),
                "SNOWFLAKE_WAREHOUSE": self.__class__.dbt_hook.extra_dejson.get(
                    "warehouse", None
                ),
                "ZETATANGO_ENV": "PRODUCTION",
                "GITLAB_USER": self.__class__.gitlab_user,
                "GITLAB_TOKEN": self.__class__.gitlab_token,
            }

        model_argument = ""
        if not models == "":
            model_argument = f"--models {models}"

        profiles = f"--profiles-dir {profiles_args}"
        target = f"--target {target_args}"
        deps = " ".join(["dbt", DbtAction.deps.name, profiles, target])
        command = " ".join(["dbt", action.name, profiles, target, model_argument])

        logging.info(f"Execute command: {command}")

        super(DbtOperator, self).__init__(
            bash_command="git clone https://${GITLAB_USER}:${GITLAB_TOKEN}@gitlab.com/tc-data/curated-data-warehouse.git"
            "&& cd curated-data-warehouse"
            "&& git pull origin master"
            f"&& {deps}"
            f"&& {command}",
            env=env,
            *args,
            **kwargs,
        )
