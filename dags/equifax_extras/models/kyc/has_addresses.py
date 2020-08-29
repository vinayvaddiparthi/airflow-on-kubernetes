from sqlalchemy.orm import relationship
from sqlalchemy import event
from sqlalchemy import and_

from .address import Address
from .address_relationship import AddressRelationship

from typing import Any, List, Optional, TYPE_CHECKING


PHYSICAL_ADDRESS = "physical_address"
LEGAL_BUSINESS_ADDRESS = "legal_business_address"


class HasAddressesType:
    @property
    def physical_addresses(self) -> List[Address]:
        return list()

    @property
    def legal_business_addresses(self) -> List[Address]:
        return list()


if TYPE_CHECKING:
    MixinBase = HasAddressesType
else:
    MixinBase = object


class HasAddresses(MixinBase):
    @property
    def physical_address(self) -> Optional[Address]:
        if not self.physical_addresses:
            return None
        return self.physical_addresses[0]

    @property
    def legal_business_address(self) -> Optional[Address]:
        if not self.legal_business_addresses:
            return None
        return self.legal_business_addresses[0]


@event.listens_for(HasAddresses, "mapper_configured", propagate=True)
def setup_listener(_mapper: Any, class_: Any) -> None:
    class_.addresses = relationship(
        Address,
        primaryjoin=class_.id == AddressRelationship.party_id,
        secondary=AddressRelationship.__tablename__,
    )

    class_.physical_addresses = relationship(
        Address,
        primaryjoin=and_(
            class_.id == AddressRelationship.party_id,
            AddressRelationship.category == PHYSICAL_ADDRESS,
        ),
        secondary=AddressRelationship.__tablename__,
    )

    class_.legal_business_addresses = relationship(
        Address,
        primaryjoin=and_(
            class_.id == AddressRelationship.party_id,
            AddressRelationship.category == LEGAL_BUSINESS_ADDRESS,
        ),
        secondary=AddressRelationship.__tablename__,
    )
