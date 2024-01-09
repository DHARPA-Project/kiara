# -*- coding: utf-8 -*-
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping, Set, Union

from sqlalchemy import Engine, create_engine, text

from kiara.models.values.value import PersistedData, SerializedData, Value
from kiara.registries import SqliteArchiveConfig
from kiara.registries.data import DataArchive
from kiara.registries.data.data_store import BaseDataStore
from kiara.utils.windows import fix_windows_longpath


class SqliteDataArchive(DataArchive):

    _archive_type_name = "sqlite_data_archive"
    _config_cls = SqliteArchiveConfig

    def __init__(self, archive_id: uuid.UUID, config: SqliteArchiveConfig):

        DataArchive.__init__(self, archive_id=archive_id, config=config)
        self._db_path: Union[Path, None] = None
        self._cached_engine: Union[Engine, None] = None
        # self._lock: bool = True

    @property
    def sqlite_path(self):

        if self._db_path is not None:
            return self._db_path

        db_path = Path(self.config.archive_path).resolve()
        self._db_path = fix_windows_longpath(db_path)

        if self._db_path.exists():
            return self._db_path

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        return self._db_path

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.sqlite_path}"

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
    metadata TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS values_data (
    chunk_id TEXT PRIMARY KEY,
    chunk_type TEXT NOT NULL,
    chunk_data BLOB NOT NULL,
    compression_type TEXT NULL,
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
        raise NotImplementedError()

    def _retrieve_value_details(self, value_id: uuid.UUID) -> Mapping[str, Any]:

        sql = text("SELECT metadata FROM values_metadata WHERE value_id = ?")
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, (str(value_id),))
            result = cursor.fetchone()
            return result[0]

        raise NotImplementedError()

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

        dbg("RETRIEVE ALL")

        sql = text("SELECT value_id FROM values_metadata")
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql)
            result = cursor.fetchall()
            return {uuid.UUID(x[0]) for x in result}

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

        # TODO: implement this
        return None

    def retrieve_chunk(
        self,
        chunk_id: str,
        as_file: Union[bool, str, None] = None,
        symlink_ok: bool = True,
    ) -> Union[bytes, str]:

        if as_file:
            raise NotImplementedError()

        sql = text("SELECT data FROM value_data WHERE chunk_id = ?")
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, (chunk_id,))
            result_bytes = cursor.fetchone()
            return result_bytes[0]


class SqliteDataStore(SqliteDataArchive, BaseDataStore):

    _archive_type_name = "sqlite_data_store"

    def _persist_environment_details(
        self, env_type: str, env_hash: str, env_data: Mapping[str, Any]
    ):
        raise NotImplementedError()

    def _persist_value_data(self, value: Value) -> PersistedData:

        serialized_value: SerializedData = value.serialized_data
        dbg(serialized_value)

        raise NotImplementedError()

    def _persist_stored_value_info(self, value: Value, persisted_value: PersistedData):
        raise NotImplementedError()

    def _persist_value_details(self, value: Value):
        raise NotImplementedError()

    def _persist_destiny_backlinks(self, value: Value):
        raise NotImplementedError()

    def _persist_value_pedigree(self, value: Value):
        raise NotImplementedError()
