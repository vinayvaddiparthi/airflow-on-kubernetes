from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import object_session
from sqlalchemy.ext.declarative import declared_attr

from typing import Any

from .applicant_attribute import ApplicantAttribute


class HasApplicantAttributes(object):
    @declared_attr
    def attributes(self) -> Any:
        return relationship("ApplicantAttribute")

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
