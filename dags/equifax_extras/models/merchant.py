from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.orm import relationship

from .base import Base

from .has_addresses import HasAddresses
from .has_applicants import HasApplicants
from .has_guid import HasGuid
from .has_id import HasId


class Merchant(Base, HasId, HasGuid, HasAddresses, HasApplicants):
    __tablename__ = "dim_merchant"

    name = Column(String)

    loans = relationship("Loan")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"Merchant({self})"
