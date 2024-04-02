# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

import orjson

from kiara import is_debug
from kiara.utils import log_message

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def get_kiara_db_url(base_path: str):

    abs_path = os.path.abspath(os.path.expanduser(base_path))
    db_url = f"sqlite+pysqlite:///{abs_path}/kiara.db"
    return db_url


# def orm_json_serialize(obj: Any) -> str:
#
#     if hasattr(obj, "json"):
#         return obj.json()
#
#     if isinstance(obj, str):
#         return obj
#     elif isinstance(obj, Mapping):
#         return orjson_dumps(obj, default=None)
#     else:
#         raise Exception(f"Unsupported type for json serialization: {type(obj)}")


def orm_json_deserialize(obj: str) -> Any:
    return orjson.loads(obj)


def create_archive_engine(
    db_path: Path, force_read_only: bool, use_wal_mode: bool
) -> "Engine":

    from sqlalchemy import create_engine, text

    # if use_wal_mode:
    #     # TODO: not sure this does anything
    #     connect_args = {"check_same_thread": False, "isolation_level": "IMMEDIATE"}
    #     execution_options = {"sqlite_wal_mode": True}
    # else:

    connect_args: Dict[str, Any] = {}
    execution_options: Dict[str, Any] = {}

    # TODO: enable this for read-only mode?
    # def _pragma_on_connect(dbapi_con, con_record):
    #     dbapi_con.execute("PRAGMA query_only = ON")

    db_url = f"sqlite+pysqlite:///{db_path.as_posix()}"
    if force_read_only:
        db_url = db_url + "?mode=ro&uri=true"

    db_engine = create_engine(
        db_url,
        future=True,
        connect_args=connect_args,
        execution_options=execution_options,
    )

    if use_wal_mode:
        with db_engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=wal;"))

    if is_debug():
        with db_engine.connect() as conn:
            wal_mode = conn.execute(text("PRAGMA journal_mode;")).fetchone()
            log_message(
                "detect.sqlite.journal_mode", result={wal_mode[0]}, db_url=db_url
            )

    return db_engine


def delete_archive_db(db_path: Path):

    db_path.unlink(missing_ok=True)
    shm_file = db_path.parent / f"{db_path.name}-shm"
    shm_file.unlink(missing_ok=True)
    wal_file = db_path.parent / f"{db_path.name}-wal"
    wal_file.unlink(missing_ok=True)


# def ensure_current_environments_persisted(
#     engine: Engine,
# ) -> Mapping[str, EnvironmentOrm]:
#
#     from kiara.kiara import EnvironmentRegistry
#
#     envs = {}
#     with engine.create_session() as session:
#         for (
#             env_name,
#             env,
#         ) in EnvironmentRegistry.instance().current_environments.items():
#
#             md = (
#                 session.query(EnvironmentOrm)
#                 .filter_by(metadata_hash=env.model_data_hash)
#                 .first()
#             )
#             if not md:
#
#                 md_schema = (
#                     session.query(MetadataSchemaOrm)
#                     .filter_by(metadata_schema_hash=env.get_schema_hash())
#                     .first()
#                 )
#                 if not md_schema:
#                     md_schema = MetadataSchemaOrm(
#                         metadata_schema_hash=env.get_schema_hash(),
#                         metadata_type=env.get_category_alias(),
#                         metadata_schema=env.schema_json(),
#                     )
#                     session.add(md_schema)
#                     session.commit()
#
#                 md = EnvironmentOrm(
#                     metadata_hash=env.model_data_hash,
#                     metadata_schema_id=md_schema.id,
#                     metadata_payload=env,
#                 )
#                 session.add(md)
#                 session.commit()
#
#             envs[env_name] = md
#
#     return envs
