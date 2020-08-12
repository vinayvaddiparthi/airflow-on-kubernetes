import json
from base64 import b64decode
from pyporky.symmetric import SymmetricPorky
from airflow.models import Variable

porky = SymmetricPorky(
    aws_region=Variable.get("aws_kms_region", default_var="ca-central-1")
)


def encrypted(func):
    def decrypt(*args):
        encrypted_value = func(*args)
        if encrypted_value is None or encrypted_value is "":
            return b""

        decode = b64decode(encrypted_value)
        ciphertext_info = json.loads(decode)

        key = b64decode(ciphertext_info["key"])
        data = b64decode(ciphertext_info["data"])
        nonce = b64decode(ciphertext_info["nonce"])

        data = porky.decrypt(enciphered_dek=key, enciphered_data=data, nonce=nonce)
        return data

    return decrypt
