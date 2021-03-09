from sqlalchemy import Table, Column, Integer

from equifax_extras.data.decorators import to_string, encrypted, marshalled
from equifax_extras.data.models.base import Base, metadata


merchant_table = Table(
    "merchant",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("guid"),
    Column("name"),
    Column("encrypted_file_number")
)


class Merchant(Base):
    __table__ = merchant_table

    def __str__(self) -> str:
        return str(self.name)

    def __repr__(self) -> str:
        return f"Merchant({self})"

    @property  # type: ignore
    @to_string
    @marshalled
    @encrypted
    def file_number(self) -> str:
        return self.encrypted_file_number
