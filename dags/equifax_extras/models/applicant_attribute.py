from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String

from .base import Base

from .encrypted import encrypted
from .has_guid import HasGuid
from .marshalled import marshalled
from .rails_model import RailsModel


class ApplicantAttribute(Base, RailsModel, HasGuid):
    __tablename__ = "applicant_attributes"

    encrypted_value = Column(String)
    encrypted_value_iv = Column(String)

    @property
    @marshalled
    @encrypted
    def value(self) -> str:
        return self.encrypted_value

    encryption_epoch = Column(Integer)

    applicant_id = Column(Integer, ForeignKey("applicants.id"))

    key = Column(String)
    partition_guid = Column(String)

    def __repr__(self) -> str:
        return f"{self.key}: {self.value}"

    def __str__(self) -> str:
        return f"{self.key}: {self.value}"
