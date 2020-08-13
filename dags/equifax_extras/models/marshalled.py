from rubymarshal.reader import loads as rubymarshal_loads

from typing import Any, Callable


def marshalled(func: Callable) -> Callable:
    def unmarshal(*args: Any):
        marshaled_value = func(*args)
        unmarshalled_value = (
            rubymarshal_loads(marshaled_value) if marshaled_value else None
        )
        return unmarshalled_value

    return unmarshal
