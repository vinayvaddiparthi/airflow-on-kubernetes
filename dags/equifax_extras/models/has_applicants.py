from sqlalchemy import event
from sqlalchemy.orm import relationship

from typing import Any

from .merchant_owner import MerchantOwner


class HasApplicants(object):
    pass


@event.listens_for(HasApplicants, "mapper_configured", propagate=True)
def setup_listener(_mapper: Any, class_: Any) -> None:
    class_.applicants = relationship(
        "Applicant",
        primaryjoin=class_.id == MerchantOwner.applicant_id,
        secondary=MerchantOwner.__tablename__,
    )
