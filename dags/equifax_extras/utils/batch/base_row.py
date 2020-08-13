from .fixed_width_column import FixedWidthColumn

from copy import deepcopy

from typing import Any


class BaseRow:
    def __init__(self, **kwargs: Any):
        for column_name in self.__class__.column_names():
            attr = getattr(self, column_name)
            attr_copy = deepcopy(attr)
            setattr(self, column_name, attr_copy)

        for (k, v) in kwargs.items():
            attr = getattr(self, k)
            attr.set(str(v))

    def __str__(self) -> str:
        column_values = list(
            filter(lambda i: type(i) == FixedWidthColumn, self.__dict__.values())
        )
        value = ""
        for v in column_values:
            value += str(v)
        value += "\n"
        return value

    @classmethod
    def column_names(cls) -> [str]:
        names = dict(
            filter(lambda i: type(i[1]) == FixedWidthColumn, cls.__dict__.items())
        ).keys()
        return names
