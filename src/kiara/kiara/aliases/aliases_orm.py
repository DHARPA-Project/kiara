# -*- coding: utf-8 -*-
import uuid
from datetime import datetime
from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy_utc import UtcDateTime
from sqlalchemy_utils import UUIDType
from typing import Optional

from kiara.kiara.orm import Base


class AliasOrm(Base):

    __tablename__ = "aliases"

    id: Column[Optional[int]] = Column(Integer, primary_key=True)
    alias: Column[str] = Column(String, index=True, nullable=False)
    created: Column[datetime] = Column(UtcDateTime(), nullable=False, index=True)
    version: Column[int] = Column(Integer, nullable=False, index=True)
    value_id: Column[Optional[uuid.UUID]] = Column(UUIDType(binary=True), nullable=True)

    UniqueConstraint(alias, version)
