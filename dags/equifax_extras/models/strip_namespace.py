import re

from sqlalchemy import types, String


class StripNamespace(types.TypeDecorator):
    impl = String

    @staticmethod
    def strip(value) -> str:
        regex = "(?:.*)::(.*)$"
        match = re.match(regex, value)
        return match[1]

    def process_literal_param(self, value, dialect) -> str:
        ret = self.__class__.strip(value)
        return ret

    def process_bind_param(self, value, dialect) -> str:
        ret = self.__class__.strip(value)
        return ret

    def process_result_value(self, value, dialect) -> str:
        ret = self.__class__.strip(value)
        return ret

    @property
    def python_type(self):
        return str
