from typing import Callable, Any


def to_string(func: Callable) -> Callable:
    def from_type(*args: Any) -> str:
        value = func(*args)
        if not value:
            return ""

        return str(value, "utf-8") if type(value) is bytes else str(value)

    return from_type
