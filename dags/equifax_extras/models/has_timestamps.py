from sqlalchemy import Column
from sqlalchemy import DateTime


class HasTimestamps(object):
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
