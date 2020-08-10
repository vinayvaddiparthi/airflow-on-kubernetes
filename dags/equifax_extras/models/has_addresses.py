from sqlalchemy.orm import relationship
from sqlalchemy import event

from sqlalchemy import and_

from .address import Address
from .address_relationship import AddressRelationship

PHYSICAL_ADDRESS = 'physical_address'
LEGAL_BUSINESS_ADDRESS = 'legal_business_address'


class HasAddresses(object):
    @property
    def physical_address(self):
        if not self.physical_addresses:
            return None
        return self.physical_addresses[0]

    @property
    def legal_business_address(self):
        if not self.legal_business_addresses:
            return None
        return self.legal_business_addresses[0]


@event.listens_for(HasAddresses, "mapper_configured", propagate=True)
def setup_listener(_mapper, class_):
    class_.addresses = relationship(
        Address,
        primaryjoin=class_.id == AddressRelationship.party_id,
        secondary="address_relationships"
    )

    class_.physical_addresses = relationship(
        Address,
        primaryjoin=and_(
            class_.id == AddressRelationship.party_id,
            AddressRelationship.category == PHYSICAL_ADDRESS
        ),
        secondary="address_relationships"
    )

    class_.legal_business_addresses = relationship(
        Address,
        primaryjoin=and_(
            class_.id == AddressRelationship.party_id,
            AddressRelationship.category == LEGAL_BUSINESS_ADDRESS
        ),
        secondary="address_relationships"
    )
