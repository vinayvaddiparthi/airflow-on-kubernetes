from copy import deepcopy

from .base_column import BaseColumn

from typing import Any, KeysView, Dict


class BaseRowMeta(type):
    @property
    def columns(cls) -> Dict:
        columns = {
            column_name: column
            for (column_name, column) in cls.__dict__.items()
            if isinstance(column, BaseColumn)
        }
        return columns

    @property
    def column_names(cls) -> KeysView:
        names = cls.columns.keys()
        return names


class BaseRow(metaclass=BaseRowMeta):
    separator = ""

    def __init__(self, **kwargs: Any):
        for column_name in self.__class__.column_names:
            attr = getattr(self, column_name)
            attr_copy = deepcopy(attr)
            setattr(self, column_name, attr_copy)

        for (column_name, value) in kwargs.items():
            if column_name in self.__class__.column_names:
                attr = getattr(self, column_name)
                attr.value = str(value)
            else:
                raise ValueError(
                    f"Unexpected value {value} for undefined column {column_name}"
                )

    @property
    def columns(self) -> Dict:
        columns = {
            column_name: getattr(self, column_name)
            for column_name in self.__class__.column_names
        }
        return columns

    @property
    def column_data(self) -> Dict:
        data = {
            column_name: str(column) for column_name, column in self.columns.items()
        }
        return data

    def __str__(self) -> str:
        values = (str(column) for column in self.columns.values())
        value = f"{self.__class__.separator.join(values)}\n"
        return value
