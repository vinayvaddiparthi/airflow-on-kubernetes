from typing import Callable, Any
from functools import wraps

from utils.french_language_pack import FrenchLanguagePack

from transliterate import translit
from transliterate.base import registry

registry.register(FrenchLanguagePack)


def transliterate(language_code: str) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any) -> str:
            value = func(*args)
            return translit(value, language_code, reversed=True)

        return wrapper

    return decorator
