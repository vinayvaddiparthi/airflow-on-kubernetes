from sqlalchemy import Column
from sqlalchemy import Integer, String

from .base import Base

from .encrypted import encrypted
from .has_addresses import HasAddresses
from .has_applicant_attributes import HasApplicantAttributes
from .has_guid import HasGuid
from .has_id import HasId


class Applicant(Base, HasId, HasGuid, HasAddresses, HasApplicantAttributes):
    __tablename__ = "dim_applicant"

    email_address_id = Column(String)

    encrypted_date_of_birth = Column(String)

    @property  # type: ignore
    @encrypted
    def date_of_birth(self) -> str:
        return self.encrypted_date_of_birth

    encrypted_first_name = Column(String)

    @property  # type: ignore
    @encrypted
    def first_name(self) -> str:
        return self.encrypted_first_name

    encrypted_last_name = Column(String)

    @property  # type: ignore
    @encrypted
    def last_name(self) -> str:
        return self.encrypted_last_name

    encrypted_middle_name = Column(String)

    @property  # type: ignore
    @encrypted
    def middle_name(self) -> str:
        return self.encrypted_middle_name
