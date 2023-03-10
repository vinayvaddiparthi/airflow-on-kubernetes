import json
from base64 import b64decode

from helpers.suspend_aws_env import SuspendAwsEnvVar
from pyporky.symmetric import SymmetricPorky

from typing import Any, Callable

with SuspendAwsEnvVar():
    porky = SymmetricPorky(aws_region="ca-central-1")


def encrypted(func: Callable) -> Callable:
    def decrypt(*args: Any) -> bytes:
        encrypted_value = func(*args)
        if not encrypted_value:
            return b""

        decode = b64decode(encrypted_value)
        if not decode:
            return b""

        ciphertext_info = json.loads(decode)
        key = b64decode(ciphertext_info["key"])
        data = b64decode(ciphertext_info["data"])
        nonce = b64decode(ciphertext_info["nonce"])

        data = porky.decrypt(enciphered_dek=key, enciphered_data=data, nonce=nonce)
        return data

    return decrypt
