# -*- coding: utf-8 -*-
from typing import Dict
import uuid
from datetime import datetime
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy_utc import UtcDateTime, utcnow
from sqlalchemy_utils import UUIDType

Base = declarative_base()


class MetadataSchemaOrm(Base):
    __tablename__ = "metadata_schema_lookup"

    id = Column(Integer, primary_key=True)
    metadata_schema_hash: int = Column(Integer, index=True)
    metadata_type: str = Column(String, nullable=False)
    metadata_schema: Dict = Column(JSON, nullable=False)
    metadata_payloads = relationship("EnvironmentOrm")

    UniqueConstraint(metadata_schema_hash)


class EnvironmentOrm(Base):
    __tablename__ = "environments"

    id = Column(Integer, primary_key=True)
    metadata_hash: int = Column(Integer, index=True, nullable=False)
    metadata_schema_id: int = Column(
        Integer, ForeignKey("metadata_schema_lookup.id"), nullable=False
    )
    metadata_payload: Dict = Column(JSON, nullable=False)

    UniqueConstraint(metadata_hash)


class ManifestOrm(Base):
    __tablename__ = "manifests"

    id = Column(Integer, primary_key=True)
    module_type: str = Column(String, index=True, nullable=False)
    module_config: Dict = Column(JSON, nullable=False)
    manifest_hash: int = Column(Integer, index=True, nullable=False)
    is_idempotent: bool = Column(Boolean, nullable=False)

    UniqueConstraint(module_type, manifest_hash)

jobs_env_association_table = Table(
    "job_environments",
    Base.metadata,
    Column("jobs_id", ForeignKey("jobs.id"), primary_key=True),
    Column("environment_id", ForeignKey("environments.id"), primary_key=True),
)


class JobsOrm(Base):

    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    manifest_id: int = Column(
        Integer, ForeignKey("manifests.id"), nullable=False
    )
    inputs: str = Column(JSON, nullable=False)
    input_hash: str = Column(String, nullable=False)
    is_idempotent: bool = Column(Boolean, nullable=False)
    created: datetime = Column(UtcDateTime(), default=utcnow(), nullable=False)
    started: datetime = Column(UtcDateTime(), nullable=True)
    duration_ms: int = Column(Integer, nullable=True)
    environments = relationship("EnvironmentOrm", secondary=jobs_env_association_table)


class ValueTypeOrm(Base):
    __tablename__ = "data_types"

    id = Column(Integer, primary_key=True)
    type_config_hash: int = Column(Integer, index=True, nullable=False)
    type_name: str = Column(String, nullable=False, index=True)
    type_config: Dict = Column(JSON, nullable=False)

    UniqueConstraint(type_config_hash, type_name)

value_env_association_table = Table(
    "value_environments",
    Base.metadata,
    Column("value_id", ForeignKey("values.id"), primary_key=True),
    Column("environment_id", ForeignKey("environments.id"), primary_key=True),
)

class ValueOrm(Base):
    __tablename__ = "values"

    id = Column(Integer, primary_key=True)
    global_id: uuid.UUID = Column(UUIDType(binary=True))
    data_type_id: int = Column(Integer, ForeignKey("data_types.id"))
    data_type_name: str = Column(Integer, index=True, nullable=False)
    value_size: int = Column(Integer, index=True, nullable=False)
    value_hash: str = Column(String, index=True, nullable=False)
    environments = relationship("EnvironmentOrm", secondary=value_env_association_table)

    UniqueConstraint(value_hash, value_size, data_type_id)


class Pedigree(Base):
    __tablename__ = "pedigrees"

    id: int = Column(Integer, primary_key=True)
    manifest_id: int = Column(Integer, ForeignKey("manifests.id"), nullable=False)
    inputs: Dict = Column(JSON, nullable=False)


class DestinyOrm(Base):
    __tablename__ = "destinies"

    id = Column(Integer, primary_key=True)
    value_id: int = Column(Integer, ForeignKey("values.id"))
    category: str = Column(String, nullable=False, index=False)
    key: str = Column(String, nullable=False, index=False)
    manifest_id: int = Column(Integer, ForeignKey("manifests.id"))
    inputs: Dict = Column(JSON, index=False, nullable=False)
    output_name: str = Column(String, index=False, nullable=False)
    destiny_value: int = Column(Integer, ForeignKey("values.id"), nullable=True)
    description: str = Column(String, nullable=True)

    UniqueConstraint(value_id, category, key)


