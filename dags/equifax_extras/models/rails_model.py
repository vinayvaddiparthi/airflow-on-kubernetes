from sqlalchemy import Column
from sqlalchemy import Integer, DateTime


class RailsModel(object):
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
