import re

from sqlalchemy import types, String

from typing import Any


class StripNamespace(types.TypeDecorator):
    impl = String

    @staticmethod
    def strip(value: str) -> str:
        regex = "(?:.*)::(.*)$"
        match = re.match(regex, value)
        return match[1]

    def process_literal_param(self, value: str, dialect: Any) -> str:
        ret = self.__class__.strip(value)
        return ret

    def process_bind_param(self, value: str, dialect: Any) -> str:
        ret = self.__class__.strip(value)
        return ret

    def process_result_value(self, value: str, dialect: Any) -> str:
        ret = self.__class__.strip(value)
        return ret

    @property
    def python_type(self) -> Any:
        return str
