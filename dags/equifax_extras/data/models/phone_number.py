from sqlalchemy import Table, Column, Integer

from equifax_extras.data.models.base import Base, metadata


phone_number_table = Table(
    "phone_number",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("area_code"),
    Column("country_code"),
    Column("phone_number"),
)


class PhoneNumber(Base):
    __table__ = phone_number_table

    def __str__(self) -> str:
        return f"{self.area_code}{self.phone_number}"

    def __repr__(self) -> str:
        return f"Address({self})"
