# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import typing
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
    create_engine,
)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship
from sqlalchemy_utc import UtcDateTime, utcnow
from sqlalchemy_utils import UUIDType

from kiara import Kiara
from kiara.defaults import KIARA_DB_URL
from kiara.module_config import ModuleConfig
from kiara.utils import orjson_dumps

Base = declarative_base()
# SessionFactory = sessionmaker()


def json_serialize(obj: typing.Any) -> str:

    if hasattr(obj, "json"):
        return obj.json()

    if isinstance(obj, str):
        return obj
    elif isinstance(obj, typing.Mapping):
        return orjson_dumps(obj, default=None)
    else:
        raise Exception(f"Unsupported type for json serialization: {type(obj)}")


def json_deserialize(obj: str) -> typing.Any:
    return {}


class JobsMgmt(object):
    def __init__(self, kiara: Kiara):

        self._kiara: Kiara = kiara
        self._engine: Engine = create_engine(
            KIARA_DB_URL,
            echo=True,
            future=True,
            json_serializer=json_serialize,
            json_deserializer=json_deserialize,
        )
        # SessionFactory.configure(bind=self._engine)

        self._environments: typing.Optional[typing.Dict[str, Environments]] = None
        # TODO: make this configurable, and smarter (e.g. when conda is present)
        self._equality_environment_keys: typing.Set[int] = set("python")

    def get_current_environment_hash(self):
        pass

    def create_session(
        self,
    ) -> Session:  # this is mostly for type hints and IDE auto-completion

        return Session(bind=self._engine, future=True)

    @property
    def current_environments(self) -> typing.Mapping[str, "Environments"]:

        if self._environments is not None:
            return self._environments

        envs = {}
        with self.create_session() as session:
            for env_name, env in self._kiara.env_mgmt.environments.items():

                md = (
                    session.query(Environments)
                    .filter_by(metadata_hash=env.model_data_hash)
                    .first()
                )
                if not md:

                    md_schema = (
                        session.query(MetadataSchemaLookup)
                        .filter_by(metadata_schema_hash=env.get_schema_hash())
                        .first()
                    )
                    if not md_schema:
                        md_schema = MetadataSchemaLookup(
                            metadata_schema_hash=env.get_schema_hash(),
                            metadata_type=env.get_category_alias(),
                            metadata_schema=env.schema_json(),
                        )
                        session.add(md_schema)
                        session.commit()

                    md = Environments(
                        metadata_hash=env.model_data_hash,
                        metadata_schema_id=md_schema.id,
                        metadata_payload=env,
                    )
                    session.add(md)
                    session.commit()

                envs[env_name] = md

        self._environments = envs
        return self._environments

    def add_job(self, module_config: ModuleConfig):

        with self.create_session() as session:
            mci = (
                session.query(Operation)
                .filter_by(module_config_hash=module_config.module_config_hash)
                .first()
            )
            if mci is None:
                mci = Operation(
                    module_config_hash=module_config.module_config_hash,
                    module_config=module_config.module_config_data,
                    is_idempotent=True,
                )
                session.add(mci)
                session.commit()

            # TODO: check if the job already exists with the environments specified
            job = Jobs(
                module_instance_id=mci.id,
                environments=list(self._environments.values()),
                is_idempotent=False,
            )
            print(job)

            session.add(job)
            session.commit()

            return str(job.id)


class MetadataSchemaLookup(Base):
    __tablename__ = "metadata_schema_lookup"

    id = Column(Integer, primary_key=True)
    metadata_schema_hash: int = Column(Integer, index=True)
    metadata_type: str = Column(String, nullable=False)
    metadata_schema: typing.Dict = Column(JSON, nullable=False)
    metadata_payloads = relationship("Environments")

    UniqueConstraint(metadata_schema_hash)


class Environments(Base):
    __tablename__ = "environments"

    id = Column(Integer, primary_key=True)
    metadata_hash: int = Column(Integer, index=True, nullable=False)
    metadata_schema_id: int = Column(
        Integer, ForeignKey("metadata_schema_lookup.id"), nullable=False
    )
    metadata_payload: typing.Dict = Column(JSON, nullable=False)

    UniqueConstraint(metadata_hash)


class Operation(Base):
    __tablename__ = "operations"

    id = Column(Integer, primary_key=True)
    module_config_hash: int = Column(Integer, index=True, nullable=False)
    module_config: typing.Dict = Column(JSON, nullable=False)
    is_idempotent: bool = Column(Boolean, nullable=False)

    UniqueConstraint(module_config_hash)


jobs_env_association_table = Table(
    "job_environments",
    Base.metadata,
    Column("jobs_id", ForeignKey("jobs.id"), primary_key=True),
    Column("environment_id", ForeignKey("environments.id"), primary_key=True),
)


class Jobs(Base):

    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    module_instance_id: int = Column(
        Integer, ForeignKey("operations.id"), nullable=False
    )
    inputs: str = Column(JSON, nullable=False)
    input_hash: str = Column(String, nullable=False)
    is_idempotent: bool = Column(Boolean, nullable=False)
    created: datetime = Column(UtcDateTime(), default=utcnow(), nullable=False)
    started: datetime = Column(UtcDateTime(), nullable=True)
    duration_ms: int = Column(Integer, nullable=True)
    environments = relationship("Environments", secondary=jobs_env_association_table)


class ValueType(Base):
    __tablename__ = "value_types"

    id = Column(Integer, primary_key=True)
    type_config_hash: int = Column(Integer, index=True, nullable=False)
    type_config: typing.Dict = Column(JSON, nullable=False)


value_env_association_table = Table(
    "value_environments",
    Base.metadata,
    Column("value_id", ForeignKey("values.id"), primary_key=True),
    Column("environment_id", ForeignKey("environments.id"), primary_key=True),
)


class Value(Base):
    __tablename__ = "values"

    id = Column(Integer, primary_key=True)
    global_id: uuid.UUID = Column(UUIDType(binary=True))
    value_type: int = Column(Integer, ForeignKey("value_types.id"))
    value_hash: str = Column(String, nullable=False)
    environments = relationship("Environments", secondary=value_env_association_table)
