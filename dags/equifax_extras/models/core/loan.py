from sqlalchemy import Column
from sqlalchemy import Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

from equifax_extras.models.base import Base
from equifax_extras.models.has_guid import HasGuid
from equifax_extras.models.has_id import HasId


class Loan(Base, HasId, HasGuid):
    __tablename__ = "dim_loan"

    sfoi_account_id = Column(String)
    state = Column(String)

    merchant_id = Column(Integer, ForeignKey("dim_merchant.id"))
    merchant = relationship("core.merchant.Merchant")
