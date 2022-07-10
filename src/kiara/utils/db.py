# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
import os
from typing import Any, Mapping

from kiara.utils.json import orjson_dumps


def get_kiara_db_url(base_path: str):

    abs_path = os.path.abspath(os.path.expanduser(base_path))
    db_url = f"sqlite+pysqlite:///{abs_path}/kiara.db"
    return db_url


def orm_json_serialize(obj: Any) -> str:

    if hasattr(obj, "json"):
        return obj.json()

    if isinstance(obj, str):
        return obj
    elif isinstance(obj, Mapping):
        return orjson_dumps(obj, default=None)
    else:
        raise Exception(f"Unsupported type for json serialization: {type(obj)}")


def orm_json_deserialize(obj: str) -> Any:
    return orjson.loads(obj)


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
