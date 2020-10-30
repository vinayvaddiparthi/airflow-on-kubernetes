from typing import Callable, Any


def to_string(func: Callable) -> Callable:
    def from_type(*args: Any) -> str:
        value = func(*args)
        if not value:
            return ""

        s = str(value, "utf-8") if type(value) is bytes else str(value)
        s = s.strip()
        return s

    return from_type
