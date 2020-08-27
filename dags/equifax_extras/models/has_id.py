from sqlalchemy import Column
from sqlalchemy import Integer


class HasId(object):
    id = Column(Integer, primary_key=True)
