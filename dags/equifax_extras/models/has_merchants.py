from sqlalchemy import event
from sqlalchemy.orm import relationship

from typing import Any

from .merchant_owner import MerchantOwner


class HasMerchants(object):
    pass


@event.listens_for(HasMerchants, "mapper_configured", propagate=True)
def setup_listener(_mapper: Any, class_: Any) -> None:
    class_.merchants = relationship(
        "Merchant",
        primaryjoin=class_.id == MerchantOwner.merchant_id,
        secondary=MerchantOwner.__tablename__,
    )
