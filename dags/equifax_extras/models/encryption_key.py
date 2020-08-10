from sqlalchemy import Column
from sqlalchemy import Integer, String, Date, DateTime
from sqlalchemy.ext.declarative import declarative_base

from pyporky.symmetric import SymmetricPorky
import json
from base64 import b64decode

Base = declarative_base()


class EncryptionKey(Base):
    __tablename__ = 'encryption_keys'

    id = Column(Integer, primary_key=True)

    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    encrypted_data_encryption_key = Column(String)

    guid = Column(String)

    key_epoch = Column(Integer)

    partition_guid = Column(String)

    version = Column(String)
