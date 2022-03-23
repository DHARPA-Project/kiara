import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, Column, String, UniqueConstraint
from sqlalchemy_utc import UtcDateTime
from sqlalchemy_utils import UUIDType

from kiara.kiara.orm import Base

class AliasOrm(Base):

    __tablename__ = "aliases"

    id: int = Column(Integer, primary_key=True)
    alias: str = Column(String, index=True)
    created: datetime = Column(UtcDateTime(), nullable=False, index=True)
    version: int = Column(Integer, nullable=False, index=True)
    value_id: Optional[uuid.UUID] = Column(UUIDType(binary=True), nullable=True)

    UniqueConstraint(alias, version)
