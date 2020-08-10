from sqlalchemy import Column
from sqlalchemy import Integer, String

from .encrypted import encrypted

from .has_guid import HasGuid
from .rails_model import RailsModel

from .base import Base


from .has_addresses import HasAddresses


class Applicant(Base, RailsModel, HasGuid, HasAddresses):
    __tablename__ = 'applicants'

    email_address_id = Column(String)

    encrypted_date_of_birth = Column(String)
    encrypted_date_of_birth_iv = Column(String)

    @property
    @encrypted
    def date_of_birth(self):
        return self.encrypted_date_of_birth

    encrypted_first_name = Column(String)
    encrypted_first_name_iv = Column(String)

    @property
    @encrypted
    def first_name(self):
        return self.encrypted_first_name

    encrypted_last_name = Column(String)
    encrypted_last_name_iv = Column(String)

    @property
    @encrypted
    def last_name(self):
        return self.encrypted_last_name

    encrypted_middle_name = Column(String)
    encrypted_middle_name_iv = Column(String)

    @property
    @encrypted
    def middle_name(self):
        return self.encrypted_middle_name

    encryption_epoch = Column(Integer)
    partition_guid = Column(String)
    partner_guid = Column(String)

    # Deprecated
    address_id = Column(String)
    individual_id = Column(String)