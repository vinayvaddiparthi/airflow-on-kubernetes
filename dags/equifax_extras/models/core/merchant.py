from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.orm import relationship

from equifax_extras.models.base import Base

from equifax_extras.models.has_guid import HasGuid
from equifax_extras.models.has_id import HasId

import equifax_extras.models.kyc as kyc


class Merchant(Base, HasId, HasGuid):
    __tablename__ = "dim_merchant"

    name = Column(String)
    loans = relationship("Loan")
    kyc_merchants = relationship("kyc.merchant.Merchant")

    @property
    def kyc_merchant(self) -> kyc.Merchant:
        return self.kyc_merchants[0]

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"Merchant({self})"
