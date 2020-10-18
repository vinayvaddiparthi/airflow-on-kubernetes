from typing import Any


class BaseColumn:
    def __init__(self) -> None:
        self._value = ""

    @property
    def value(self) -> str:
        return str(self._value)

    @value.setter
    def value(self, value: Any) -> None:
        self._value = value
