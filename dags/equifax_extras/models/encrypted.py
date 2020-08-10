import json
from base64 import b64decode
from pyporky.symmetric import SymmetricPorky
import os

os.environ["AWS_ACCESS_KEY_ID"] = 'AKIAI6CT7DNHO7PW6IIA'
os.environ["AWS_SECRET_ACCESS_KEY"] = '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'
print(os.environ["AWS_ACCESS_KEY_ID"])
print(os.environ["AWS_SECRET_ACCESS_KEY"])
porky = SymmetricPorky(aws_region="ca-central-1")


def encrypted(func):
    def decrypt(*args):
        encrypted_value = func(*args)
        if encrypted_value is None or encrypted_value is '':
            return ''

        decode = b64decode(encrypted_value)
        ciphertext_info = json.loads(decode)

        key = b64decode(ciphertext_info['key'])
        data = b64decode(ciphertext_info['data'])
        nonce = b64decode(ciphertext_info['nonce'])

        d = porky.decrypt(
            enciphered_dek=key,
            enciphered_data=data,
            nonce=nonce
        )

        data = str(d, "utf-8")

        return data

    return decrypt
