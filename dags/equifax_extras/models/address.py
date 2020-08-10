from sqlalchemy import Column
from sqlalchemy import String, Float

from .has_guid import HasGuid
from .rails_model import RailsModel

from .base import Base


class Address(Base, RailsModel, HasGuid):
    __tablename__ = 'addresses'

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
    def lines(self):
        lines = []
        if self.civic_line != '':
            lines.append(self.civic_line)
        if self.post_box_line != '':
            lines.append(self.post_box_line)
        lines.append(self.municipal_line)
        return lines

    @property
    def country_name(self):
        return 'Canada'

    @property
    def civic_line(self):
        if self.premise_number == '' and self.thoroughfare == '':
            return ''

        if self.sub_premise_type != '' and self.sub_premise_number != '':
            return f"{self.premise_number} {self.thoroughfare} {self.sub_premise_type} {self.sub_premise_number}"
        elif self.sub_premise_number != '':
            return f"{self.sub_premise_number}-{self.premise_number} {self.thoroughfare}"
        else:
            return f"{self.premise_number} {self.thoroughfare}"

    @property
    def post_box_line(self):
        if self.post_box_type == '' and self.post_box_number == '':
            return ''

        return f"{self.post_box_type} {self.post_box_number}"

    @property
    def municipal_line(self):
        return f"{self.city} {self.state_province} {self.country_alpha_3} {self.postal_code}"

    def __repr__(self):
        separator = ' '
        return separator.join(self.lines)

    def __str__(self):
        separator = ' '
        return separator.join(self.lines)
