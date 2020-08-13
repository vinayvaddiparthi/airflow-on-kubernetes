from sqlalchemy import Column
from sqlalchemy import String, Float

from .base import Base

from .has_guid import HasGuid
from .rails_model import RailsModel

from typing import List


class Address(Base, RailsModel, HasGuid):
    __tablename__ = "addresses"

    city = Column(String)
    country_alpha_3 = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    post_box_number = Column(String)
    post_box_type = Column(String)
    postal_code = Column(String)
    premise_number = Column(String)
    state_province = Column(String)
    sub_premise_number = Column(String)
    sub_premise_type = Column(String)
    thoroughfare = Column(String)
    verified = Column(String)

    @property
    def lines(self) -> List[str]:
        lines = list()
        if self.civic_line:
            lines.append(self.civic_line)
        if self.post_box_line:
            lines.append(self.post_box_line)
        lines.append(self.municipal_line)
        return lines

    @property
    def country_name(self) -> str:
        return "Canada"

    @property
    def civic_line(self) -> str:
        if not self.premise_number and not self.thoroughfare:
            return ""

        if self.sub_premise_type and self.sub_premise_number:
            return f"{self.premise_number} {self.thoroughfare} {self.sub_premise_type} {self.sub_premise_number}"
        elif self.sub_premise_number != "":
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

    def __repr__(self) -> str:
        separator = " "
        return separator.join(self.lines)

    def __str__(self) -> str:
        separator = " "
        return separator.join(self.lines)
