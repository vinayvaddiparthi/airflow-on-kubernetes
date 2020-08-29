from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy_utils import generic_relationship

from equifax_extras.models.base import Base

from equifax_extras.models.has_guid import HasGuid
from equifax_extras.models.has_id import HasId
from equifax_extras.models.has_timestamps import HasTimestamps


class AddressRelationship(Base, HasId, HasTimestamps, HasGuid):
    __tablename__ = "dim_address_relationship"

    active = Column(String)
    category = Column(String)
    deactivated_at = Column(String)

    address_id = Column(Integer, ForeignKey("dim_address.id"))
    party_id = Column(Integer)
    party_type = Column(String)

    address = relationship("Address")
    party = generic_relationship(party_type, party_id)
