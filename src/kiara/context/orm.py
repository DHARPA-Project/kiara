# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# import uuid
# from datetime import datetime
# from sqlalchemy import (
#     JSON,
#     Boolean,
#     Column,
#     ForeignKey,
#     Integer,
#     String,
#     Table,
#     UniqueConstraint,
# )
# from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
# from sqlalchemy.orm import relationship
# from sqlalchemy_utc import UtcDateTime, utcnow
# from sqlalchemy_utils import UUIDType
# from typing import Any, Dict, List, Union
#
# Base: DeclarativeMeta = declarative_base()
#
#
# class MetadataSchemaOrm(Base):
#     __tablename__ = "metadata_schema_lookup"
#
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     metadata_schema_hash: Column[int] = Column(Integer, index=True, nullable=False)
#     metadata_type: Column[str] = Column(String, nullable=False)
#     metadata_schema: Column[Union[Dict[Any, Any], List[Any]]] = Column(
#         JSON, nullable=False
#     )
#     metadata_payloads = relationship("EnvironmentOrm")
#
#     UniqueConstraint(metadata_schema_hash)
#
#
# class EnvironmentOrm(Base):
#     __tablename__ = "environments"
#
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     metadata_hash: Column[int] = Column(Integer, index=True, nullable=False)
#     metadata_schema_id = Column(
#         Integer, ForeignKey("metadata_schema_lookup.id"), nullable=False
#     )
#     metadata_payload: Column[Union[Dict[Any, Any], List[Any]]] = Column(
#         JSON, nullable=False
#     )
#
#     UniqueConstraint(metadata_hash)
#
#
# class ManifestOrm(Base):
#     __tablename__ = "manifests"
#
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     module_type: Column[str] = Column(String, index=True, nullable=False)
#     module_config: Column[Union[Dict[Any, Any], List[Any]]] = Column(
#         JSON, nullable=False
#     )
#     manifest_hash: Column[int] = Column(Integer, index=True, nullable=False)
#     is_idempotent: Column[bool] = Column(Boolean, nullable=False)
#
#     UniqueConstraint(module_type, manifest_hash)
#
#
# jobs_env_association_table = Table(
#     "job_environments",
#     Base.metadata,
#     Column("jobs_id", ForeignKey("jobs.id"), primary_key=True),
#     Column("environment_id", ForeignKey("environments.id"), primary_key=True),
# )
#
#
# class JobsOrm(Base):
#
#     __tablename__ = "jobs"
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     manifest_id: Column[int] = Column(
#         Integer, ForeignKey("manifests.id"), nullable=False
#     )
#     inputs: Column[Union[Dict[Any, Any], List[Any]]] = Column(JSON, nullable=False)
#     input_hash: Column[str] = Column(String, nullable=False)
#     is_idempotent: Column[bool] = Column(Boolean, nullable=False)
#     created: Column[datetime] = Column(UtcDateTime(), default=utcnow(), nullable=False)
#     started: Column[Union[datetime, None]] = Column(UtcDateTime(), nullable=True)
#     duration_ms: Column[Union[int, None]] = Column(Integer, nullable=True)
#     environments = relationship("EnvironmentOrm", secondary=jobs_env_association_table)
#
#
# class ValueTypeOrm(Base):
#     __tablename__ = "data_types"
#
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     type_config_hash: Column[int] = Column(Integer, index=True, nullable=False)
#     type_name: Column[str] = Column(String, nullable=False, index=True)
#     type_config: Column[Union[Dict[Any, Any], List[Any]]] = Column(JSON, nullable=False)
#
#     UniqueConstraint(type_config_hash, type_name)
#
#
# value_env_association_table = Table(
#     "value_environments",
#     Base.metadata,
#     Column("value_id", ForeignKey("values.id"), primary_key=True),
#     Column("environment_id", ForeignKey("environments.id"), primary_key=True),
# )
#
#
# class ValueOrm(Base):
#     __tablename__ = "values"
#
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     global_id: Column[uuid.UUID] = Column(UUIDType(binary=True), nullable=False)
#     data_type_id: Column[int] = Column(
#         Integer, ForeignKey("data_types.id"), nullable=False
#     )
#     data_type_name: Column[str] = Column(String, index=True, nullable=False)
#     value_size: Column[int] = Column(Integer, index=True, nullable=False)
#     value_hash: Column[str] = Column(String, index=True, nullable=False)
#     environments = relationship("EnvironmentOrm", secondary=value_env_association_table)
#
#     UniqueConstraint(value_hash, value_size, data_type_id)
#
#
# class Pedigree(Base):
#     __tablename__ = "pedigrees"
#
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     manifest_id: Column[int] = Column(
#         Integer, ForeignKey("manifests.id"), nullable=False
#     )
#     inputs: Column[Union[Dict[Any, Any], List[Any]]] = Column(JSON, nullable=False)
#
#
# class DestinyOrm(Base):
#     __tablename__ = "destinies"
#
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     value_id: Column[int] = Column(Integer, ForeignKey("values.id"), nullable=False)
#     category: Column[str] = Column(String, nullable=False, index=False)
#     key: Column[str] = Column(String, nullable=False, index=False)
#     manifest_id: Column[int] = Column(
#         Integer, ForeignKey("manifests.id"), nullable=False
#     )
#     inputs: Column[Union[Dict[Any, Any], List[Any]]] = Column(
#         JSON, index=False, nullable=False
#     )
#     output_name: Column[str] = Column(String, index=False, nullable=False)
#     destiny_value: Column[Union[int, None]] = Column(
#         Integer, ForeignKey("values.id"), nullable=True
#     )
#     description: Column[Union[str, None]] = Column(String, nullable=True)
#
#     UniqueConstraint(value_id, category, key)
#
#
# class AliasOrm(Base):
#
#     __tablename__ = "aliases"
#
#     id: Column[Union[int, None]] = Column(Integer, primary_key=True)
#     alias: Column[str] = Column(String, index=True, nullable=False)
#     created: Column[datetime] = Column(UtcDateTime(), nullable=False, index=True)
#     version: Column[int] = Column(Integer, nullable=False, index=True)
#     value_id: Column[Union[uuid.UUID, None]] = Column(
#         UUIDType(binary=True), nullable=True
#     )
#
#     UniqueConstraint(alias, version)
