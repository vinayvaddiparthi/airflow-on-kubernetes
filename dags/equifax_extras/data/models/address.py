from sqlalchemy import Table, Column, Integer
from sqlalchemy.orm import column_property

from equifax_extras.data.decorators import to_string
from equifax_extras.data.models.base import Base, metadata

from typing import List


address_table = Table(
    "address",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("city"),
    Column("country_alpha_3"),
    Column("post_box_number"),
    Column("post_box_type"),
    Column("postal_code"),
    Column("premise_number"),
    Column("state_province"),
    Column("sub_premise_number"),
    Column("sub_premise_type"),
    Column("thoroughfare"),
)


class Address(Base):
    __table__ = address_table

    _city = column_property(address_table.c.city)
    _country_alpha_3 = column_property(address_table.c.country_alpha_3)
    _post_box_number = column_property(address_table.c.post_box_number)
    _post_box_type = column_property(address_table.c.post_box_type)
    _postal_code = column_property(address_table.c.postal_code)
    _premise_number = column_property(address_table.c.premise_number)
    _state_province = column_property(address_table.c.state_province)
    _sub_premise_number = column_property(address_table.c.sub_premise_number)
    _sub_premise_type = column_property(address_table.c.sub_premise_type)
    _thoroughfare = column_property(address_table.c.thoroughfare)

    @property  # type: ignore
    @to_string
    def city(self) -> str:
        return self._city

    @property  # type: ignore
    @to_string
    def country_alpha_3(self) -> str:
        return self._country_alpha_3

    @property  # type: ignore
    @to_string
    def post_box_number(self) -> str:
        return self._post_box_number

    @property  # type: ignore
    @to_string
    def post_box_type(self) -> str:
        return self._post_box_type

    @property  # type: ignore
    @to_string
    def postal_code(self) -> str:
        return self._postal_code

    @property  # type: ignore
    @to_string
    def premise_number(self) -> str:
        return self._premise_number

    @property  # type: ignore
    @to_string
    def state_province(self) -> str:
        return self._state_province

    @property  # type: ignore
    @to_string
    def sub_premise_number(self) -> str:
        return self._sub_premise_number

    @property  # type: ignore
    @to_string
    def sub_premise_type(self) -> str:
        return self._sub_premise_type

    @property  # type: ignore
    @to_string
    def thoroughfare(self) -> str:
        return self._thoroughfare

    @property
    def lines(self) -> List[str]:
        lines = []
        if self.civic_line:
            lines.append(self.civic_line)
        if self.post_box_line:
            lines.append(self.post_box_line)
        return lines

    @property
    def civic_line(self) -> str:
        if not self.premise_number and not self.thoroughfare:
            return ""

        if self.sub_premise_type and self.sub_premise_number:
            return f"{self.premise_number} {self.thoroughfare} {self.sub_premise_type} {self.sub_premise_number}"
        elif self.sub_premise_number:
            return (
                f"{self.sub_premise_number}-{self.premise_number} {self.thoroughfare}"
            )
        else:
            return f"{self.premise_number} {self.thoroughfare}"

    @property
    def post_box_line(self) -> str:
        if not self.post_box_type and not self.post_box_number:
            return ""

        return f"{self.post_box_type} {self.post_box_number}"

    @property
    def municipal_line(self) -> str:
        return f"{self.city} {self.state_province} {self.country_alpha_3} {self.postal_code}"

    def __str__(self) -> str:
        lines = self.lines
        lines.append(self.municipal_line)
        return " ".join(lines)

    def __repr__(self) -> str:
        return f"Address({self})"
