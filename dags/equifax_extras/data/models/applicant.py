from datetime import datetime
from typing import Optional

from sqlalchemy import Table, Column, Integer

from equifax_extras.data.models.base import Base, metadata
from equifax_extras.data.decorators import encrypted, marshalled, to_string


applicant_table = Table(
    "applicant",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("guid"),
    Column("encrypted_date_of_birth"),
    Column("encrypted_first_name"),
    Column("encrypted_last_name"),
    Column("encrypted_middle_name"),
    Column("encrypted_sin"),
    Column("encrypted_suffix"),
)


class Applicant(Base):
    __table__ = applicant_table

    @property  # type: ignore
    @to_string
    @encrypted
    def _date_of_birth(self) -> str:
        return self.encrypted_date_of_birth

    @property
    def date_of_birth(self) -> Optional[datetime]:
        try:
            return datetime.strptime(self._date_of_birth, "%Y-%m-%d")
        except ValueError:
            return None

    @property  # type: ignore
    @to_string
    @encrypted
    def first_name(self) -> str:
        return self.encrypted_first_name

    @property  # type: ignore
    @to_string
    @encrypted
    def last_name(self) -> str:
        return self.encrypted_last_name

    @property  # type: ignore
    @to_string
    @encrypted
    def middle_name(self) -> str:
        return self.encrypted_middle_name

    @property  # type: ignore
    @to_string
    @marshalled
    @encrypted
    def sin(self) -> str:
        return self.encrypted_sin

    @property  # type: ignore
    @to_string
    @marshalled
    @encrypted
    def suffix(self) -> str:
        return self.encrypted_suffix

    def __str__(self) -> str:
        return str(self.first_name) + " " + str(self.last_name)

    def __repr__(self) -> str:
        return f"Applicant({self})"
