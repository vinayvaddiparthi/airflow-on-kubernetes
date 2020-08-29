from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String

from equifax_extras.models.base import Base

from equifax_extras.models.encrypted import encrypted
from equifax_extras.models.has_id import HasId
from equifax_extras.models.marshalled import marshalled


class ApplicantAttribute(Base, HasId):
    __tablename__ = "dim_applicant_attribute"

    key = Column(String)
    encrypted_value = Column(String)

    @property  # type: ignore
    @marshalled
    @encrypted
    def value(self) -> str:
        return self.encrypted_value

    applicant_id = Column(Integer, ForeignKey("dim_kyc_applicant.id"))

    def __repr__(self) -> str:
        return f"{self.key}: {self.value}"

    def __str__(self) -> str:
        return f"{self.key}: {self.value}"
