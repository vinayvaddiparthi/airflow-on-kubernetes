from sqlalchemy import Column
from sqlalchemy import String

from .base import Base

from .encrypted import encrypted
from .has_addresses import HasAddresses
from .has_applicant_attributes import HasApplicantAttributes
from .has_guid import HasGuid
from .has_id import HasId
from .has_merchants import HasMerchants


class Applicant(
    Base, HasId, HasGuid, HasAddresses, HasApplicantAttributes, HasMerchants
):
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

    def __str__(self) -> str:
        return str(self.first_name) + " " + str(self.last_name)

    def __repr__(self) -> str:
        return f"Applicant({self})"
