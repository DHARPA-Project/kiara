# -*- coding: utf-8 -*-
import os
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable, Mapping, Set, Union

from orjson import orjson
from sqlalchemy import Engine, create_engine, text

from kiara.defaults import kiara_app_dirs
from kiara.models.values.value import PersistedData, Value
from kiara.registries import SqliteArchiveConfig
from kiara.registries.data import DataArchive
from kiara.registries.data.data_store import BaseDataStore
from kiara.utils.hashfs import shard
from kiara.utils.json import orjson_dumps
from kiara.utils.windows import fix_windows_longpath


class SqliteDataArchive(DataArchive[SqliteArchiveConfig]):

    _archive_type_name = "sqlite_data_archive"
    _config_cls = SqliteArchiveConfig

    @classmethod
    def _load_store_config(
        cls, store_uri: str, allow_write_access: bool, **kwargs
    ) -> Union[SqliteArchiveConfig, None]:

        if allow_write_access:
            return None

        if not Path(store_uri).is_file():
            return None

        import sqlite3

        con = sqlite3.connect(store_uri)

        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {x[0] for x in cursor.fetchall()}
        con.close()

        if tables != {
            "values_pedigree",
            "values_destinies",
            "archive_metadata",
            "persisted_values",
            "values_metadata",
            "values_data",
            "environments",
        }:
            return None

        config = SqliteArchiveConfig(sqlite_db_path=store_uri)
        return config

    def __init__(
        self,
        archive_alias: str,
        archive_config: SqliteArchiveConfig,
        force_read_only: bool = False,
    ):

        DataArchive.__init__(
            self,
            archive_alias=archive_alias,
            archive_config=archive_config,
            force_read_only=force_read_only,
        )
        self._db_path: Union[Path, None] = None
        self._cached_engine: Union[Engine, None] = None
        self._data_cache_dir = Path(kiara_app_dirs.user_cache_dir) / "data" / "chunks"
        self._data_cache_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._cache_dir_depth = 2
        self._cache_dir_width = 1
        self._value_id_cache: Union[Iterable[uuid.UUID], None] = None
        # self._lock: bool = True

    def _retrieve_archive_id(self) -> uuid.UUID:
        sql = text("SELECT value FROM archive_metadata WHERE key='archive_id'")

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql)
            row = result.fetchone()
            if row is None:
                raise Exception("No archive ID found in metadata")
            return uuid.UUID(row[0])

    @property
    def sqlite_path(self):

        if self._db_path is not None:
            return self._db_path

        db_path = Path(self.config.sqlite_db_path).resolve()
        self._db_path = fix_windows_longpath(db_path)

        if self._db_path.exists():
            return self._db_path

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        return self._db_path

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.sqlite_path}"

    def get_chunk_path(self, chunk_id: str) -> Path:

        chunk_id = chunk_id.replace("-", "")
        chunk_id = chunk_id.lower()

        prefix = chunk_id[0:5]
        rest = chunk_id[5:]

        paths = shard(rest, self._cache_dir_depth, self._cache_dir_width)

        chunk_path = Path(os.path.join(self._data_cache_dir, prefix, *paths))
        return chunk_path

    @property
    def sqlite_engine(self) -> "Engine":

        if self._cached_engine is not None:
            return self._cached_engine

        # def _pragma_on_connect(dbapi_con, con_record):
        #     dbapi_con.execute("PRAGMA query_only = ON")

        self._cached_engine = create_engine(self.db_url, future=True)
        create_table_sql = """
CREATE TABLE IF NOT EXISTS values_metadata (
    value_id TEXT PRIMARY KEY,
    value_hash TEXT NOT NULL,
    value_size INTEGER NOT NULL,
    data_type_name TEXT NOT NULL,
    value_metadata TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS persisted_values (
    value_id TEXT PRIMARY KEY,
    value_hash TEXT NOT NULL,
    value_size INTEGER NOT NULL,
    data_type_name TEXT NOT NULL,
    persisted_value_metadata TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS values_data (
    chunk_id TEXT PRIMARY KEY,
    chunk_data BLOB NOT NULL,
    compression_type TEXT NULL
);
CREATE TABLE IF NOT EXISTS values_pedigree (
    value_id TEXT NOT NULL PRIMARY KEY,
    pedigree TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS values_destinies (
    value_id TEXT NOT NULL,
    destiny_name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS environments (
    environment_type TEXT NOT NULL,
    environment_hash TEXT NOT NULL,
    environment_data TEXT NOT NULL,
    PRIMARY KEY (environment_type, environment_hash)
);
"""

        with self._cached_engine.begin() as connection:
            for statement in create_table_sql.split(";"):
                if statement.strip():
                    connection.execute(text(statement))

        # if self._lock:
        #     event.listen(self._cached_engine, "connect", _pragma_on_connect)
        return self._cached_engine

    def _retrieve_serialized_value(self, value: Value) -> PersistedData:

        value_id = str(value.value_id)
        sql = text(
            "SELECT persisted_value_metadata FROM persisted_values WHERE value_id = :value_id"
        )
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, {"value_id": value_id})
            result = cursor.fetchone()
            data = orjson.loads(result[0])
            return PersistedData(**data)

    def _retrieve_value_details(self, value_id: uuid.UUID) -> Mapping[str, Any]:

        sql = text(
            "SELECT value_metadata FROM values_metadata WHERE value_id = :value_id"
        )
        params = {"value_id": str(value_id)}
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, params)
            result = cursor.fetchone()
            data = orjson.loads(result[0])
            return data

    def _retrieve_environment_details(
        self, env_type: str, env_hash: str
    ) -> Mapping[str, Any]:

        sql = text(
            "SELECT environment_data FROM environments_data WHERE environment_type = ? AND environment_hash = ?"
        )
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, (env_type, env_hash))
            result = cursor.fetchone()
            return result[0]

    # def find_values(self, matcher: ValueMatcher) -> Iterable[Value]:
    #     raise NotImplementedError()

    def _retrieve_all_value_ids(
        self, data_type_name: Union[str, None] = None
    ) -> Union[None, Iterable[uuid.UUID]]:

        if self._value_id_cache is not None:
            return self._value_id_cache

        sql = text("SELECT value_id FROM values_metadata")
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql)
            result = cursor.fetchall()
            result_set = {uuid.UUID(x[0]) for x in result}
            self._value_id_cache = result_set
            return result_set

    def _find_values_with_hash(
        self,
        value_hash: str,
        value_size: Union[int, None] = None,
        data_type_name: Union[str, None] = None,
    ) -> Union[Set[uuid.UUID], None]:

        if value_size is not None:
            raise NotImplementedError()
        if data_type_name is not None:
            raise NotImplementedError()

        sql = text("SELECT value_id FROM values_metadata WHERE value_hash = ?")
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, (value_hash,))
            result = cursor.fetchall()
            return {uuid.UUID(x[0]) for x in result}

    def _find_destinies_for_value(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Union[Mapping[str, uuid.UUID], None]:

        sql = text(
            "SELECT destiny_name FROM values_destinies WHERE value_id = :value_id"
        )
        params = {"value_id": str(value_id)}
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, params)
            result = cursor.fetchall()
            result_destinies = {x[0]: value_id for x in result}
            return result_destinies

    def retrieve_chunk(
        self,
        chunk_id: str,
        as_file: Union[bool, str, None] = None,
        symlink_ok: bool = True,
    ) -> Union[bytes, str]:

        if as_file:
            chunk_path = self.get_chunk_path(chunk_id)

            if chunk_path.exists():
                return chunk_path.as_posix()

        sql = text("SELECT chunk_data FROM values_data WHERE chunk_id = :chunk_id")
        params = {"chunk_id": chunk_id}
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, params)
            result_bytes = cursor.fetchone()

        if not as_file:
            return result_bytes[0]

        chunk_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with open(chunk_path, "wb") as file:
            file.write(result_bytes[0])

        return chunk_path.as_posix()

    def _delete_archive(self):
        os.unlink(self.sqlite_path)


class SqliteDataStore(SqliteDataArchive, BaseDataStore):

    _archive_type_name = "sqlite_data_store"

    @classmethod
    def _load_store_config(
        cls, store_uri: str, allow_write_access: bool, **kwargs
    ) -> Union[SqliteArchiveConfig, None]:

        if not allow_write_access:
            return None

        if not Path(store_uri).is_file():
            return None

        import sqlite3

        con = sqlite3.connect(store_uri)

        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {x[0] for x in cursor.fetchall()}
        con.close()

        if tables != {
            "values_pedigree",
            "values_destinies",
            "archive_metadata",
            "persisted_values",
            "values_metadata",
            "values_data",
            "environments",
        }:
            return None

        config = SqliteArchiveConfig(sqlite_db_path=store_uri)
        return config

    def _persist_environment_details(
        self, env_type: str, env_hash: str, env_data: Mapping[str, Any]
    ):

        sql = text(
            "INSERT OR IGNORE INTO environments (environment_type, environment_hash, environment_data) VALUES (:environment_type, :environment_hash, :environment_data)"
        )
        env_data_json = orjson_dumps(env_data)
        with self.sqlite_engine.connect() as conn:
            params = {
                "environment_type": env_type,
                "environment_hash": env_hash,
                "environment_data": env_data_json,
            }
            conn.execute(sql, params)
            conn.commit()
        # print(env_type)
        # print(env_hash)
        # print(env_data_json)
        # raise NotImplementedError()

    # def _persist_value_data(self, value: Value) -> PersistedData:
    #
    #     serialized_value: SerializedData = value.serialized_data
    #     dbg(serialized_value.model_dump())
    #     dbg(serialized_value.get_keys())
    #
    #     raise NotImplementedError()

    def _persist_chunk(self, chunk_id: str, chunk: Union[str, BytesIO]):

        sql = text(
            "SELECT EXISTS(SELECT 1 FROM values_data WHERE chunk_id = :chunk_id)"
        )
        with self.sqlite_engine.connect() as conn:
            result = conn.execute(sql, {"chunk_id": chunk_id}).scalar()
            if result:
                return

        if isinstance(chunk, str):
            with open(chunk, "rb") as file:
                file_data = file.read()
                bytes_io = BytesIO(file_data)
        else:
            bytes_io = chunk

        sql = text(
            "INSERT INTO values_data (chunk_id, chunk_data) VALUES (:chunk_id, :chunk_data)"
        )
        with self.sqlite_engine.connect() as conn:
            params = {"chunk_id": chunk_id, "chunk_data": bytes_io.getvalue()}

            conn.execute(sql, params)
            conn.commit()

    def _persist_stored_value_info(self, value: Value, persisted_value: PersistedData):

        self._value_id_cache = None

        value_id = str(value.value_id)
        value_hash = value.value_hash
        value_size = value.value_size
        data_type_name = value.data_type_name

        metadata = persisted_value.model_dump_json()

        sql = text(
            "INSERT INTO persisted_values (value_id, value_hash, value_size, data_type_name, persisted_value_metadata) VALUES (:value_id, :value_hash, :value_size, :data_type_name, :metadata)"
        )

        with self.sqlite_engine.connect() as conn:
            params = {
                "value_id": value_id,
                "value_hash": value_hash,
                "value_size": value_size,
                "data_type_name": data_type_name,
                "metadata": metadata,
            }
            conn.execute(sql, params)
            conn.commit()

    def _persist_value_details(self, value: Value):

        value_id = str(value.value_id)
        value_hash = value.value_hash
        value_size = value.value_size
        data_type_name = value.data_type_name

        metadata = value.model_dump_json()

        sql = text(
            "INSERT INTO values_metadata (value_id, value_hash, value_size, data_type_name, value_metadata) VALUES (:value_id, :value_hash, :value_size, :data_type_name, :metadata)"
        )
        with self.sqlite_engine.connect() as conn:
            params = {
                "value_id": value_id,
                "value_hash": value_hash,
                "value_size": value_size,
                "data_type_name": data_type_name,
                "metadata": metadata,
            }
            conn.execute(sql, params)
            conn.commit()

    def _persist_destiny_backlinks(self, value: Value):

        value_id = str(value.value_id)

        with self.sqlite_engine.connect() as conn:

            for destiny_value_id, destiny_name in value.destiny_backlinks.items():

                sql = text(
                    "INSERT INTO values_destinies (value_id, destiny_name) VALUES (:value_id, :destiny_name)"
                )
                params = {
                    "value_id": value_id,
                    "destiny_name": destiny_name,
                }
                conn.execute(sql, params)

            conn.commit()

    def _persist_value_pedigree(self, value: Value):

        value_id = str(value.value_id)
        pedigree = value.pedigree.manifest_data_as_json()

        sql = text(
            "INSERT INTO values_pedigree (value_id, pedigree) VALUES (:value_id, :pedigree)"
        )
        with self.sqlite_engine.connect() as conn:
            params = {"value_id": value_id, "pedigree": pedigree}
            conn.execute(sql, params)
            conn.commit()
