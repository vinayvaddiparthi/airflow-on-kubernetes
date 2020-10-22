from sqlalchemy import Table, Column, Integer

from equifax_extras.data.models.base import Base, metadata


merchant_table = Table(
    "merchant",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("guid"),
    Column("name"),
)


class Merchant(Base):
    __table__ = merchant_table

    def __str__(self) -> str:
        return str(self.name)

    def __repr__(self) -> str:
        return f"Merchant({self})"
