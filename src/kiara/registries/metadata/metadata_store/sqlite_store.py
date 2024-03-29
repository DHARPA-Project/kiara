# -*- coding: utf-8 -*-
import uuid
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple, Union, Iterable, Generator

import orjson
from sqlalchemy import text
from sqlalchemy.engine import Engine

from kiara.exceptions import KiaraException
from kiara.registries import SqliteArchiveConfig
from kiara.registries.metadata import MetadataArchive, MetadataStore, MetadataMatcher
from kiara.utils.dates import get_current_time_incl_timezone
from kiara.utils.db import create_archive_engine, delete_archive_db

REQUIRED_METADATA_TABLES = {
    "metadata",
}


class SqliteMetadataArchive(MetadataArchive):

    _archive_type_name = "sqlite_metadata_archive"
    _config_cls = SqliteArchiveConfig

    @classmethod
    def _load_archive_config(
        cls, archive_uri: str, allow_write_access: bool, **kwargs
    ) -> Union[Dict[str, Any], None]:

        if allow_write_access:
            return None

        if not Path(archive_uri).is_file():
            return None

        import sqlite3

        con = sqlite3.connect(archive_uri)

        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {x[0] for x in cursor.fetchall()}
        con.close()

        if not REQUIRED_METADATA_TABLES.issubset(tables):
            return None

        # config = SqliteArchiveConfig(sqlite_db_path=store_uri)
        return {"sqlite_db_path": archive_uri}

    def __init__(
        self,
        archive_name: str,
        archive_config: SqliteArchiveConfig,
        force_read_only: bool = False,
    ):

        super().__init__(
            archive_name=archive_name,
            archive_config=archive_config,
            force_read_only=force_read_only,
        )
        self._db_path: Union[Path, None] = None
        self._cached_engine: Union[Engine, None] = None
        self._use_wal_mode: bool = archive_config.use_wal_mode

        # self._lock: bool = True

    # def _retrieve_archive_id(self) -> uuid.UUID:
    #     sql = text("SELECT value FROM archive_metadata WHERE key='archive_id'")
    #
    #     with self.sqlite_engine.connect() as connection:
    #         result = connection.execute(sql)
    #         row = result.fetchone()
    #         if row is None:
    #             raise Exception("No archive ID found in metadata")
    #         return uuid.UUID(row[0])

    def _retrieve_archive_metadata(self) -> Mapping[str, Any]:

        sql = text("SELECT key, value FROM archive_metadata")

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql)
            return {row[0]: row[1] for row in result}

    @property
    def sqlite_path(self):

        if self._db_path is not None:
            return self._db_path

        db_path = Path(self.config.sqlite_db_path).resolve()
        # self._db_path = fix_windows_longpath(db_path)
        self._db_path = db_path

        if self._db_path.exists():
            return self._db_path

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        return self._db_path

    # @property
    # def db_url(self) -> str:
    #     return f"sqlite:///{self.sqlite_path}"

    @property
    def sqlite_engine(self) -> "Engine":

        if self._cached_engine is not None:
            return self._cached_engine

        self._cached_engine = create_archive_engine(
            db_path=self.sqlite_path,
            force_read_only=self.is_force_read_only(),
            use_wal_mode=self._use_wal_mode,
        )

        create_table_sql = """
CREATE TABLE IF NOT EXISTS metadata_schemas (
    model_schema_hash TEXT PRIMARY KEY,
    model_type_id TEXT NOT NULL,
    model_schema TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metadata (
    metadata_item_id TEXT PRIMARY KEY,
    metadata_item_created TEXT NOT NULL,
    metadata_item_key TEXT NOT NULL,
    metadata_item_hash TEXT NOT NULL,
    model_type_id TEXT NOT NULL,
    model_schema_hash TEXT NOT NULL,
    metadata_value TEXT NOT NULL,
    FOREIGN KEY (model_schema_hash) REFERENCES metadata_schemas (model_schema_hash),
    UNIQUE (metadata_item_key, metadata_item_hash)
);
CREATE TABLE IF NOT EXISTS metadata_references (
    reference_item_type TEXT NOT NULL,
    reference_item_key TEXT NOT NULL,
    reference_item_id TEXT NOT NULL,
    reference_created TEXT NOT NULL,
    metadata_item_id TEXT NOT NULL,
    FOREIGN KEY (metadata_item_id) REFERENCES metadata (metadata_item_id)
);
"""

        with self._cached_engine.begin() as connection:
            for statement in create_table_sql.split(";"):
                if statement.strip():
                    connection.execute(text(statement))

        # if self._lock:
        #     event.listen(self._cached_engine, "connect", _pragma_on_connect)
        return self._cached_engine

    def _find_matching_metadata_items(self, matcher: "MetadataMatcher",
                                          metadata_item_result_fields: Union[Iterable[str], None] = None,
                                          reference_item_result_fields: Union[Iterable[str], None] = None) -> Generator[Tuple[Any, ...], None, None]:

        # find all metadata items first

        if not metadata_item_result_fields:
            metadata_fields_str = "m.*"
        else:
            metadata_fields_str = ", ".join((f"m.{x}" for x in metadata_item_result_fields))

        metadata_fields_str += ", :result_type as result_type"

        sql_string = f"SELECT {metadata_fields_str} FROM metadata m "
        conditions = []
        params = {
            "result_type": "metadata_item"
        }

        ref_query = False
        if matcher.reference_item_types or matcher.reference_item_keys or matcher.reference_item_ids:
            ref_query = True
            sql_string += "JOIN metadata_references r ON m.metadata_item_id = r.metadata_item_id"

        if matcher.metadata_item_keys:
            conditions.append("WHERE m.metadata_item_key in :metadata_item_keys")
            params["metadata_item_key"] = matcher.metadata_item_keys

        if matcher.reference_item_ids:
            assert ref_query
            in_clause = []
            for idx, item_id in enumerate(matcher.reference_item_ids):
                params[f"ri_id_{idx}"] = item_id
                in_clause.append(f":ri_id_{idx}")
            in_clause_str = ", ".join(in_clause)
            conditions.append(f"WHERE r.reference_item_id IN ({in_clause_str})")
            params["reference_item_ids"] = tuple(matcher.reference_item_ids)

        if matcher.reference_item_types:
            assert ref_query
            conditions.append("AND r.reference_item_type IN :reference_item_types")
            params["reference_item_types"] = tuple(matcher.reference_item_types)

        if matcher.reference_item_keys:
            assert ref_query
            conditions.append("AND r.reference_item_key IN :reference_item_keys")
            params["reference_item_keys"] = tuple(matcher.reference_item_keys)

        if conditions:

            for cond in conditions:
                sql_string += f" {cond} AND"

            sql_string = sql_string[:-4]
        sql = text(sql_string)

        # ... now construct the query to find the reference items (if applicable)
        if not reference_item_result_fields:
            reference_fields_str = "*"
        else:
            reference_fields_str = ", ".join((f"r.{x}" for x in reference_item_result_fields))


        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql, params)
            for row in result:
                yield row


    def _retrieve_referenced_metadata_item_data(
        self, key: str, reference_type: str, reference_key: str, reference_id: str
    ) -> Union[Tuple[str, Mapping[str, Any]], None]:

        sql = text(
            """
            SELECT m.model_type_id, m.metadata_value
            FROM metadata m
            JOIN metadata_references r ON m.metadata_item_id = r.metadata_item_id
            WHERE r.reference_item_type = :reference_type AND r.reference_item_key = :reference_key AND r.reference_item_id = :reference_id and m.metadata_item_key = :key
        """
        )

        with self.sqlite_engine.connect() as connection:
            parmas = {
                "reference_type": reference_type,
                "reference_key": reference_key,
                "reference_id": reference_id,
                "key": key,
            }
            result = connection.execute(sql, parmas)
            row = result.fetchall()
            if not row:
                return None

            if len(row) > 1:
                msg = f"Multiple ({len(row)}) metadata items found for key '{key}'"
                if reference_type:
                    msg += f" and reference type '{reference_type}'"
                if reference_id:
                    msg += f" and reference id '{reference_id}'"
                msg += "."
                raise KiaraException(msg)

            data_str = row[0][1]
            data = orjson.loads(data_str)

            return (row[0][0], data)

    def _delete_archive(self):

        delete_archive_db(db_path=self.sqlite_path)


class SqliteMetadataStore(SqliteMetadataArchive, MetadataStore):

    _archive_type_name = "sqlite_metadata_store"

    @classmethod
    def _load_archive_config(
        cls, archive_uri: str, allow_write_access: bool, **kwargs
    ) -> Union[Dict[str, Any], None]:

        if not allow_write_access:
            return None

        if not Path(archive_uri).is_file():
            return None

        import sqlite3

        con = sqlite3.connect(archive_uri)

        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {x[0] for x in cursor.fetchall()}
        con.close()

        if not REQUIRED_METADATA_TABLES.issubset(tables):
            return None

        # config = SqliteArchiveConfig(sqlite_db_path=store_uri)
        return {"sqlite_db_path": archive_uri}

    def _set_archive_metadata_value(self, key: str, value: Any):
        """Set custom metadata for the archive."""

        sql = text(
            "INSERT OR REPLACE INTO archive_metadata (key, value) VALUES (:key, :value)"
        )
        with self.sqlite_engine.connect() as conn:
            params = {"key": key, "value": value}
            conn.execute(sql, params)
            conn.commit()

    def _store_metadata_schema(
        self, model_schema_hash: str, model_type_id: str, model_schema: str
    ):

        sql = text(
            "INSERT OR IGNORE INTO metadata_schemas (model_schema_hash, model_type_id, model_schema) VALUES (:model_schema_hash, :model_type_id, :model_schema)"
        )
        params = {
            "model_schema_hash": model_schema_hash,
            "model_type_id": model_type_id,
            "model_schema": model_schema,
        }
        with self.sqlite_engine.connect() as conn:
            conn.execute(sql, params)
            conn.commit()

    def _store_metadata_item(
        self,
        key: str,
        value_json: str,
        value_hash: str,
        model_type_id: str,
        model_schema_hash: str,
    ) -> uuid.UUID:

        from kiara.registries.ids import ID_REGISTRY

        metadata_item_created = get_current_time_incl_timezone().isoformat()

        sql = text(
            "INSERT OR IGNORE INTO metadata (metadata_item_id, metadata_item_created, metadata_item_key, metadata_item_hash, model_type_id, model_schema_hash, metadata_value) VALUES (:metadata_item_id, :metadata_item_created, :metadata_item_key, :metadata_item_hash, :model_type_id, :model_schema_hash, :metadata_value)"
        )

        metadata_item_id = ID_REGISTRY.generate(
            comment="new provisional metadata item id"
        )

        params = {
            "metadata_item_id": str(metadata_item_id),
            "metadata_item_created": metadata_item_created,
            "metadata_item_key": key,
            "metadata_item_hash": value_hash,
            "model_type_id": model_type_id,
            "model_schema_hash": model_schema_hash,
            "metadata_value": value_json,
        }

        query_metadata_id = text(
            "SELECT metadata_item_id FROM metadata WHERE metadata_item_key = :metadata_item_key AND metadata_item_hash = :metadata_item_hash"
        )
        query_metadata_params = {
            "metadata_item_key": key,
            "metadata_item_hash": value_hash,
        }

        with self.sqlite_engine.connect() as conn:
            conn.execute(sql, params)
            result = conn.execute(query_metadata_id, query_metadata_params)
            metadata_item_id = uuid.UUID(result.fetchone()[0])
            conn.commit()

        return metadata_item_id

    def _store_metadata_reference(
        self,
        reference_item_type: str,
        reference_item_key: str,
        reference_item_id: str,
        metadata_item_id: str,
        replace_existing_references: bool = False,
        allow_multiple_references: bool = False,
    ) -> None:

        if not replace_existing_references:
            raise NotImplementedError(
                "not replacing existing metadata references is not yet supported"
            )

        else:

            sql_replace = text(
                "DELETE FROM metadata_references WHERE reference_item_type = :reference_item_type AND reference_item_key = :reference_item_key AND reference_item_id = :reference_item_id"
            )
            sql_replace_params = {
                "reference_item_type": reference_item_type,
                "reference_item_key": reference_item_key,
                "reference_item_id": reference_item_id,
            }

            metadata_reference_created = get_current_time_incl_timezone().isoformat()
            sql_insert = text(
                "INSERT INTO metadata_references (reference_item_type, reference_item_key, reference_item_id, reference_created, metadata_item_id) VALUES (:reference_item_type, :reference_item_key, :reference_item_id, :reference_created, :metadata_item_id)"
            )
            sql_insert_params = {
                "reference_item_type": reference_item_type,
                "reference_item_key": reference_item_key,
                "reference_item_id": reference_item_id,
                "reference_created": metadata_reference_created,
                "metadata_item_id": metadata_item_id,
            }
            with self.sqlite_engine.connect() as conn:
                conn.execute(sql_replace, sql_replace_params)
                conn.execute(sql_insert, sql_insert_params)
                conn.commit()
