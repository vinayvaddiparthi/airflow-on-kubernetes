from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import event
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import object_session
from sqlalchemy.ext.declarative import declared_attr

from equifax_extras.models.base import Base
from equifax_extras.models.encrypted import encrypted
from equifax_extras.models.has_guid import HasGuid
from equifax_extras.models.has_id import HasId
from .applicant_attribute import ApplicantAttribute
from .has_addresses import HasAddresses

from typing import Any


class HasApplicantAttributes(object):
    @declared_attr
    def attributes(self) -> Any:
        return relationship(ApplicantAttribute)

    def attribute(self, key: str) -> str:
        attribute = (
            object_session(self)
            .query(ApplicantAttribute)
            .with_parent(self)
            .filter(ApplicantAttribute.key == key)
            .first()
        )
        value = attribute.value if attribute else ""
        return value


class HasMerchants(object):
    pass


@event.listens_for(HasMerchants, "mapper_configured", propagate=True)
def setup_listener(_mapper: Any, class_: Any) -> None:
    class_.merchants = relationship(
        "kyc.merchant.Merchant",
        primaryjoin="kyc.applicant.Applicant.id == foreign(MerchantRelationship.party_id)",
        secondaryjoin="kyc.merchant.Merchant.id == foreign(MerchantRelationship.merchant_id)",
        secondary="dim_merchant_relationship",
    )


class Applicant(
    Base, HasId, HasGuid, HasAddresses, HasApplicantAttributes, HasMerchants
):
    __tablename__ = "dim_kyc_applicant"

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
