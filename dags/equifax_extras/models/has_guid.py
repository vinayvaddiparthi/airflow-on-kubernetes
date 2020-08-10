from sqlalchemy import Column
from sqlalchemy import String


class HasGuid(object):
    guid = Column(String)
