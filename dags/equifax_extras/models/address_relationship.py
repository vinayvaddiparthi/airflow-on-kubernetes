from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy_utils import generic_relationship

from .base import Base

from .has_guid import HasGuid
from .rails_model import RailsModel

from .address import Address


class AddressRelationship(Base, RailsModel, HasGuid):
    __tablename__ = "address_relationships"

    active = Column(String)
    category = Column(String)
    deactivated_at = Column(String)

    address_id = Column(Integer, ForeignKey(f"{Address.__tablename__}.id"))
    party_id = Column(Integer)
    party_type = Column(String)

    address = relationship("Address")
    party = generic_relationship(party_type, party_id)
