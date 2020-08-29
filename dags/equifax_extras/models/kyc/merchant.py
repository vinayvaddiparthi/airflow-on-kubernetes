from sqlalchemy import Column, ForeignKey
from sqlalchemy import String
from sqlalchemy import event
from sqlalchemy.orm import relationship

from equifax_extras.models.base import Base
from equifax_extras.models.has_id import HasId
from .has_addresses import HasAddresses

from typing import Any


class HasApplicants(object):
    pass


@event.listens_for(HasApplicants, "mapper_configured", propagate=True)
def setup_listener(_mapper: Any, class_: Any) -> None:
    class_.applicants = relationship(
        "Applicant",
        primaryjoin="kyc.merchant.Merchant.id == foreign(MerchantRelationship.merchant_id)",
        secondaryjoin="kyc.applicant.Applicant.id == foreign(MerchantRelationship.party_id)",
        secondary="dim_merchant_relationship",
    )
    class_.owners = relationship(
        "Applicant",
        primaryjoin="and_("
        "   kyc.merchant.Merchant.id == foreign(MerchantRelationship.merchant_id),"
        "   MerchantRelationship.relation_type == 'ownership'"
        ")",
        secondaryjoin="kyc.applicant.Applicant.id == foreign(MerchantRelationship.party_id)",
        secondary="dim_merchant_relationship",
    )


class Merchant(Base, HasId, HasAddresses, HasApplicants):
    __tablename__ = "dim_kyc_merchant"

    name = Column(String)
    guid = Column(String, ForeignKey("dim_merchant.guid"))

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"Merchant({self})"
