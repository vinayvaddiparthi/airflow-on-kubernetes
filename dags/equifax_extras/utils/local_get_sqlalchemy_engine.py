from typing import Dict, Optional, Any

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def local_get_sqlalchemy_engine(
    self: SnowflakeHook, engine_kwargs: Optional[Dict[Any, Any]] = None
) -> Engine:
    engine_kwargs = engine_kwargs or {
        "connect_args": {"authenticator": "externalbrowser"}
    }
    return create_engine(self.get_uri(), **engine_kwargs)
