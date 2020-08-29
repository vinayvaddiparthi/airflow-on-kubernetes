from sqlalchemy import Column
from sqlalchemy import Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy_utils import generic_relationship

from equifax_extras.models.base import Base
from equifax_extras.models.has_id import HasId
from equifax_extras.models.strip_namespace import StripNamespace


class MerchantRelationship(Base, HasId):
    __tablename__ = "dim_merchant_relationship"

    merchant_id = Column(Integer, ForeignKey("dim_kyc_merchant.id"))
    party_id = Column(Integer)
    party_type = Column(StripNamespace(String))

    merchant = relationship("kyc.merchant.Merchant")
    party = generic_relationship(party_type, party_id)

    relation_type = Column(String)
