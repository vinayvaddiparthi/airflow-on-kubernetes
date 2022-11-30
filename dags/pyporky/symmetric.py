from functools import lru_cache
from typing import Dict, Optional, Tuple

from nacl.secret import SecretBox


class SymmetricPorky:
    CMK_KEY_ORIGIN = "AWS_KMS"
    CMK_KEY_USAGE = "ENCRYPT_DECRYPT"
    SYMMETRIC_KEY_SPEC = "AES_256"

    def __init__(
        self,
        kms_client,
    ):
        self.kms = kms_client

    def decrypt(
        self,
        enciphered_dek: bytes,
        enciphered_data: bytes,
        nonce: bytes,
        encryption_context: Optional[Dict[str, str]] = None,
    ) -> bytes:
        key = self._decrypt_data_encryption_key(enciphered_dek, encryption_context)  # type: ignore
        secret_box = SecretBox(key)
        return secret_box.decrypt(enciphered_data, nonce)

    def encrypt(
        self,
        data: bytes,
        cmk_key_id: Optional[str] = None,
        enciphered_dek: Optional[bytes] = None,
        encryption_context: Optional[Dict[str, str]] = None,
    ) -> Tuple[bytes, bytes, bytes]:
        if enciphered_dek and not cmk_key_id:
            plaintext_key = self._decrypt_data_encryption_key(
                enciphered_dek, encryption_context  # type: ignore
            )
            enciphered_key: bytes = enciphered_dek
        elif cmk_key_id and not enciphered_dek:
            plaintext_key, enciphered_key = self._generate_data_encryption_key(
                cmk_key_id, encryption_context
            )
        else:
            raise Exception(
                "Either `enciphered_dek` or `cmk_key_id` must be provided as an argument to this method"
            )

        secret_box = SecretBox(plaintext_key)

        encrypted = secret_box.encrypt(data)
        return enciphered_key, encrypted.ciphertext, encrypted.nonce

    def _generate_data_encryption_key(
        self, cmk_key_id: str, encryption_context: Optional[Dict[str, str]] = None
    ) -> Tuple[bytes, bytes]:
        resp = (
            self.kms.generate_data_key(
                KeyId=cmk_key_id,
                EncryptionContext=encryption_context,
                KeySpec=self.SYMMETRIC_KEY_SPEC,
            )
            if encryption_context
            else self.kms.generate_data_key(
                KeyId=cmk_key_id, KeySpec=self.SYMMETRIC_KEY_SPEC
            )
        )
        return resp["Plaintext"], resp["CiphertextBlob"]

    @lru_cache(maxsize=4096)
    def _decrypt_data_encryption_key(
        self, ciphertext_key: bytes, encryption_context: Optional[Dict] = None
    ) -> bytes:
        return (
            self.kms.decrypt(
                CiphertextBlob=ciphertext_key, EncryptionContext=encryption_context
            )["Plaintext"]
            if encryption_context
            else self.kms.decrypt(CiphertextBlob=ciphertext_key)["Plaintext"]
        )
