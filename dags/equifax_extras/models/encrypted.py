import json
from base64 import b64decode
from pyporky.symmetric import SymmetricPorky

porky = SymmetricPorky(aws_region="ca-central-1")


def encrypted(func):
    def decrypt(*args):
        encrypted_value = func(*args)
        if not encrypted_value:
            return b""

        decode = b64decode(encrypted_value)
        ciphertext_info = json.loads(decode)

        key = b64decode(ciphertext_info["key"])
        data = b64decode(ciphertext_info["data"])
        nonce = b64decode(ciphertext_info["nonce"])

        data = porky.decrypt(enciphered_dek=key, enciphered_data=data, nonce=nonce)
        return data

    return decrypt
