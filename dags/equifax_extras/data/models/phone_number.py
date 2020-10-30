from sqlalchemy import Table, Column, Integer
from sqlalchemy.orm import column_property

from equifax_extras.data.decorators import to_string
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

    _area_code = column_property(phone_number_table.c.area_code)
    _country_code = column_property(phone_number_table.c.country_code)
    _phone_number = column_property(phone_number_table.c.phone_number)

    @property  # type: ignore
    @to_string
    def area_code(self) -> str:
        return self._area_code

    @property  # type: ignore
    @to_string
    def country_code(self) -> str:
        return self._country_code

    @property  # type: ignore
    @to_string
    def phone_number(self) -> str:
        return self._phone_number

    def __str__(self) -> str:
        return f"{self.area_code}{self.phone_number}"

    def __repr__(self) -> str:
        return f"Address({self})"
