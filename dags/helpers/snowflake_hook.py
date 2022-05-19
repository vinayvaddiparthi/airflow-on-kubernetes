import os

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from sqlalchemy.engine import Engine

from typing import Dict


def test_get_sqlalchemy_engine(self: SnowflakeHook, engine_kwargs: Dict = {}) -> Engine:
    connection_name = getattr(self, self.conn_name_attr)

    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    role = os.environ.get("SNOWFLAKE_ROLE")

    zetatango_database = "ZETATANGO"
    production_database = "ANALYTICS_PRODUCTION"

    zetatango_engine = create_engine(
        URL(account=account, database=zetatango_database, role=role, user=user),
        connect_args={"authenticator": "externalbrowser"},
    )
    production_engine = create_engine(
        URL(account=account, database=production_database, role=role, user=user,),
        connect_args={"authenticator": "externalbrowser"},
    )

    if connection_name == "snowflake_zetatango_production":
        return zetatango_engine
    else:
        return production_engine
