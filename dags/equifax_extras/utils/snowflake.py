from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

from sqlalchemy.engine import Engine

from typing import Any, Optional


def get_engine(
    snowflake_connection: str,
    snowflake_kwargs: Optional[Any] = None,
    engine_kwargs: Optional[Any] = None,
) -> Engine:
    if snowflake_kwargs is None:
        snowflake_kwargs = dict()
    if engine_kwargs is None:
        engine_kwargs = dict()
    engine = SnowflakeHook(
        snowflake_connection, **snowflake_kwargs
    ).get_sqlalchemy_engine(engine_kwargs)
    return engine


def get_local_engine(
    snowflake_connection: str, snowflake_kwargs: Optional[Any] = None
) -> Engine:
    engine_kwargs = {"connect_args": {"authenticator": "externalbrowser"}}
    engine = get_engine(
        snowflake_connection,
        snowflake_kwargs=snowflake_kwargs,
        engine_kwargs=engine_kwargs,
    )
    return engine
