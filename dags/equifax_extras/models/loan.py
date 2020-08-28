from sqlalchemy import Column
from sqlalchemy import Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

from .base import Base

from .has_guid import HasGuid
from .has_id import HasId


class Loan(Base, HasId, HasGuid):
    __tablename__ = "dim_loan"

    state = Column(String)

    merchant_id = Column(Integer, ForeignKey("dim_merchant.id"))
    merchant = relationship("Merchant")
