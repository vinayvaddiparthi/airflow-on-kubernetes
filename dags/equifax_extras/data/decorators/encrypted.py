import json
import boto3
from base64 import b64decode
from airflow.models import Variable
from typing import Any, Callable

from pyporky.symmetric import SymmetricPorky

sts_client = boto3.client("sts", region_name="ca-central-1")
assumed_role_object = sts_client.assume_role(
    RoleArn=Variable.get("zetatango_decryption_role"),
    RoleSessionName="AssumeRoleSession",
)
task_session = boto3.session.Session(
    aws_access_key_id=assumed_role_object["Credentials"]["AccessKeyId"],
    aws_secret_access_key=assumed_role_object["Credentials"]["SecretAccessKey"],
    aws_session_token=assumed_role_object["Credentials"]["SessionToken"],
)

kms_client = task_session.client('kms')
porky = SymmetricPorky(kms_client)


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
