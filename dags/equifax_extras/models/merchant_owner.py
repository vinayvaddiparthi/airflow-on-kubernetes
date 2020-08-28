from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import ForeignKey

from .base import Base
from .has_id import HasId


class MerchantOwner(Base, HasId):
    __tablename__ = "dim_merchant_owner"

    merchant_id = Column(Integer, ForeignKey("dim_merchant.id"))
    applicant_id = Column(Integer, ForeignKey("dim_applicant.id"))
