# -*- coding: utf-8 -*-
import os
import uuid
from io import BytesIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Generic,
    Iterable,
    List,
    Mapping,
    Sequence,
    Set,
    Union,
)

import orjson
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from kiara.defaults import (
    CHUNK_CACHE_BASE_DIR,
    CHUNK_CACHE_DIR_DEPTH,
    CHUNK_CACHE_DIR_WIDTH,
    CHUNK_COMPRESSION_TYPE,
    REQUIRED_TABLES_DATA_ARCHIVE,
    TABLE_NAME_ARCHIVE_METADATA,
    TABLE_NAME_DATA_CHUNKS,
    TABLE_NAME_DATA_DESTINIES,
    TABLE_NAME_DATA_METADATA,
    TABLE_NAME_DATA_PEDIGREE,
    TABLE_NAME_DATA_SERIALIZATION_METADATA,
)
from kiara.models.values.value import PersistedData, Value
from kiara.registries import (
    ARCHIVE_CONFIG_CLS,
    ArchiveDetails,
    SqliteArchiveConfig,
    SqliteDataStoreConfig,
)
from kiara.registries.data import DataArchive
from kiara.registries.data.data_store import BaseDataStore
from kiara.utils.db import create_archive_engine, delete_archive_db
from kiara.utils.hashfs import shard

if TYPE_CHECKING:
    from multiformats import CID
    from multiformats.varint import BytesLike


class SqliteDataArchive(DataArchive[SqliteArchiveConfig], Generic[ARCHIVE_CONFIG_CLS]):

    _archive_type_name = "sqlite_data_archive"
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

        required_tables = REQUIRED_TABLES_DATA_ARCHIVE

        if not required_tables.issubset(tables):
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
        self._data_cache_dir = CHUNK_CACHE_BASE_DIR
        self._data_cache_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        self._cache_dir_depth = CHUNK_CACHE_DIR_DEPTH
        self._cache_dir_width = CHUNK_CACHE_DIR_WIDTH
        self._value_id_cache: Union[Iterable[uuid.UUID], None] = None
        self._use_wal_mode: bool = archive_config.use_wal_mode
        # self._lock: bool = True

    def _retrieve_archive_metadata(self) -> Mapping[str, Any]:

        sql = text(f"SELECT key, value FROM {TABLE_NAME_ARCHIVE_METADATA}")

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql)
            return {row[0]: row[1] for row in result}

    # def _retrieve_archive_id(self) -> uuid.UUID:
    #     sql = text("SELECT value FROM archive_metadata WHERE key='archive_id'")
    #
    #     with self.sqlite_engine.connect() as connection:
    #         result = connection.execute(sql)
    #         row = result.fetchone()
    #         if row is None:
    #             raise Exception("No archive ID found in metadata")
    #         return uuid.UUID(row[0])

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

        self._cached_engine = create_archive_engine(
            db_path=self.sqlite_path,
            force_read_only=self.is_force_read_only(),
            use_wal_mode=self._use_wal_mode,
        )

        create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME_DATA_METADATA} (
    value_id TEXT PRIMARY KEY,
    value_hash TEXT NOT NULL,
    value_size INTEGER NOT NULL,
    value_created TEXT NOT NULL,
    data_type_name TEXT NOT NULL,
    value_metadata TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS {TABLE_NAME_DATA_SERIALIZATION_METADATA} (
    value_id TEXT PRIMARY KEY,
    value_hash TEXT NOT NULL,
    value_size INTEGER NOT NULL,
    data_type_name TEXT NOT NULL,
    persisted_value_metadata TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS {TABLE_NAME_DATA_CHUNKS} (
    chunk_id TEXT PRIMARY KEY,
    chunk_data BLOB NOT NULL,
    compression_type INTEGER NULL
);
CREATE TABLE IF NOT EXISTS {TABLE_NAME_DATA_PEDIGREE} (
    value_id TEXT NOT NULL PRIMARY KEY,
    pedigree TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS {TABLE_NAME_DATA_DESTINIES} (
    value_id TEXT NOT NULL,
    destiny_name TEXT NOT NULL
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
            f"SELECT persisted_value_metadata FROM {TABLE_NAME_DATA_SERIALIZATION_METADATA} WHERE value_id = :value_id"
        )
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, {"value_id": value_id})
            result = cursor.fetchone()
            data = orjson.loads(result[0])
            return PersistedData(**data)

    def _retrieve_value_details(self, value_id: uuid.UUID) -> Mapping[str, Any]:

        sql = text(
            f"SELECT value_metadata FROM {TABLE_NAME_DATA_METADATA} WHERE value_id = :value_id"
        )
        params = {"value_id": str(value_id)}
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, params)
            result = cursor.fetchone()
            data: Mapping[str, Any] = orjson.loads(result[0])
            return data

    # def _retrieve_environment_details(
    #     self, env_type: str, env_hash: str
    # ) -> Mapping[str, Any]:
    #
    #     sql = text(
    #         "SELECT environment_data FROM environments_data WHERE environment_type = ? AND environment_hash = ?"
    #     )
    #     with self.sqlite_engine.connect() as conn:
    #         cursor = conn.execute(sql, (env_type, env_hash))
    #         result = cursor.fetchone()
    #         return result[0]  # type: ignore

    # def find_values(self, matcher: ValueMatcher) -> Iterable[Value]:
    #     raise NotImplementedError()

    def has_value(self, value_id: uuid.UUID) -> bool:
        """
        Check whether the specific value_id is persisted in this data store.

        Implementing classes are encouraged to override this method, and choose a suitable, implementation specific
        way to quickly determine whether a value id is valid for this data store.

        Arguments:
        ---------
            value_id: the id of the value to check.


        Returns:
        -------
            whether this data store contains the value with the specified id
        """

        sql_text = text(
            f"SELECT EXISTS(SELECT 1 FROM {TABLE_NAME_DATA_METADATA} WHERE value_id = :value_id)"
        )
        with self.sqlite_engine.connect() as conn:
            result = conn.execute(sql_text, {"value_id": str(value_id)}).scalar()
            return bool(result)

    def _retrieve_all_value_ids(
        self, data_type_name: Union[str, None] = None
    ) -> Union[None, Iterable[uuid.UUID]]:

        if self._value_id_cache is not None:
            return self._value_id_cache

        sql = text(f"SELECT value_id FROM {TABLE_NAME_DATA_METADATA}")
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql)
            result = cursor.fetchall()
            result_set = {uuid.UUID(x[0]) for x in result}
            self._value_id_cache = result_set
            return result_set

    def retrieve_all_chunk_ids(self) -> Iterable[str]:

        sql = text(f"SELECT chunk_id FROM {TABLE_NAME_DATA_CHUNKS}")
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql)
            result = cursor.fetchall()
            return {x[0] for x in result}

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

        sql = text(
            f"SELECT value_id FROM {TABLE_NAME_DATA_METADATA} WHERE value_hash = :value_hash"
        )
        params = {
            "value_hash": value_hash,
        }
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, parameters=params)
            result = cursor.fetchall()
            return {uuid.UUID(x[0]) for x in result}

    def _find_destinies_for_value(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Union[Mapping[str, uuid.UUID], None]:

        sql = text(
            f"SELECT destiny_name FROM {TABLE_NAME_DATA_DESTINIES} WHERE value_id = :value_id"
        )
        params = {"value_id": str(value_id)}
        with self.sqlite_engine.connect() as conn:
            cursor = conn.execute(sql, params)
            result = cursor.fetchall()
            result_destinies = {x[0]: value_id for x in result}
            return result_destinies

    # def retrieve_chunk(
    #     self,
    #     chunk_id: str,
    #     as_file: Union[bool, str, None] = None,
    #     symlink_ok: bool = True,
    # ) -> Union[bytes, str]:
    #
    #     import lzma
    #
    #     import lz4.frame
    #     from zstandard import ZstdDecompressor
    #
    #     dctx = ZstdDecompressor()
    #
    #     if as_file:
    #         chunk_path = self.get_chunk_path(chunk_id)
    #
    #         if chunk_path.exists():
    #             return chunk_path.as_posix()
    #
    #     sql = text(
    #         "SELECT chunk_data, compression_type FROM values_data WHERE chunk_id = :chunk_id"
    #     )
    #     params = {"chunk_id": chunk_id}
    #     with self.sqlite_engine.connect() as conn:
    #         cursor = conn.execute(sql, params)
    #         result_bytes = cursor.fetchone()
    #
    #     chunk_data: Union[str, bytes] = result_bytes[0]
    #     compression_type = result_bytes[1]
    #     if compression_type not in (None, 0):
    #         if CHUNK_COMPRESSION_TYPE(compression_type) == CHUNK_COMPRESSION_TYPE.ZSTD:
    #             chunk_data = dctx.decompress(chunk_data)
    #         elif (
    #             CHUNK_COMPRESSION_TYPE(compression_type) == CHUNK_COMPRESSION_TYPE.LZMA
    #         ):
    #             chunk_data = lzma.decompress(chunk_data)
    #         elif CHUNK_COMPRESSION_TYPE(compression_type) == CHUNK_COMPRESSION_TYPE.LZ4:
    #             chunk_data = lz4.frame.decompress(chunk_data)
    #         else:
    #             raise ValueError(f"Unsupported compression type: {compression_type}")
    #
    #     if not as_file:
    #         return chunk_data
    #
    #     chunk_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    #     with open(chunk_path, "wb") as file:
    #         file.write(chunk_data)
    #
    #     return chunk_path.as_posix()

    def retrieve_chunks(
        self,
        chunk_ids: Sequence[str],
        as_files: Union[bool, None] = None,
        symlink_ok: bool = True,
    ) -> Generator[Union["BytesLike", str], None, None]:

        import lzma

        import lz4.frame
        from zstandard import ZstdDecompressor

        dctx = ZstdDecompressor()

        MAX_CHUNKS_SQL = 50

        with self.sqlite_engine.connect() as conn:

            def retrieve_missing_chunks(
                missing_ids: List[str],
            ) -> Generator[Union["BytesLike", str], None, None]:

                id_list_str = ", ".join("'" + item + "'" for item in missing_ids)
                sql = text(
                    f"""SELECT chunk_id, chunk_data, compression_type FROM {TABLE_NAME_DATA_CHUNKS}
                    WHERE
                        chunk_id in ({id_list_str})
                    ORDER BY
                      CASE chunk_id
                        {"".join([f"WHEN '{id}' THEN {i} " for i, id in enumerate(missing_ids)])}
                      END
                    """
                )

                result = conn.execute(sql)
                for row in result:
                    result_chunk_id = row[0]
                    chunk_data = row[1]
                    compression_type = row[2]
                    if compression_type not in (None, 0):
                        if (
                            CHUNK_COMPRESSION_TYPE(compression_type)
                            == CHUNK_COMPRESSION_TYPE.ZSTD
                        ):
                            chunk_data = dctx.decompress(chunk_data)
                        elif (
                            CHUNK_COMPRESSION_TYPE(compression_type)
                            == CHUNK_COMPRESSION_TYPE.LZMA
                        ):
                            chunk_data = lzma.decompress(chunk_data)
                        elif (
                            CHUNK_COMPRESSION_TYPE(compression_type)
                            == CHUNK_COMPRESSION_TYPE.LZ4
                        ):
                            chunk_data = lz4.frame.decompress(chunk_data)
                        else:
                            raise ValueError(
                                f"Unsupported compression type: {compression_type}"
                            )

                    chunk_id = missing_ids.pop(0)
                    assert result_chunk_id == chunk_id

                    if not as_files:
                        yield chunk_data
                    else:
                        chunk_path = self.get_chunk_path(chunk_id)
                        chunk_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                        with open(chunk_path, "wb") as file:
                            file.write(chunk_data)
                        yield chunk_path.as_posix()

            missing_chunk_ids: List[str] = []

            for idx, chunk_id in enumerate(chunk_ids):

                if as_files:
                    chunk_path = self.get_chunk_path(chunk_id)

                    if chunk_path.exists():
                        path = chunk_path.as_posix()

                        if missing_chunk_ids:
                            for chunk in retrieve_missing_chunks(missing_chunk_ids):
                                yield chunk
                            assert not missing_chunk_ids

                        yield path
                        continue

                missing_chunk_ids.append(chunk_id)
                if len(missing_chunk_ids) >= MAX_CHUNKS_SQL:
                    for chunk in retrieve_missing_chunks(missing_chunk_ids):
                        yield chunk
                    assert not missing_chunk_ids

            if missing_chunk_ids:
                for chunk in retrieve_missing_chunks(missing_chunk_ids):
                    yield chunk
                assert not missing_chunk_ids

    def _delete_archive(self):

        delete_archive_db(db_path=self.sqlite_path)

    def get_archive_details(self) -> ArchiveDetails:

        size = self.sqlite_path.stat().st_size
        all_values = self.value_ids

        if all_values is not None:
            _all_values = list(all_values)
            details = {
                "no_values": len(_all_values),
                "value_ids": sorted((str(x) for x in _all_values)),
                "dynamic_archive": False,
                "size": size,
            }
        else:
            # will probably never happen
            details = {"dynamic_archive": True, "size": size}

        return ArchiveDetails(root=details)


class SqliteDataStore(SqliteDataArchive[SqliteDataStoreConfig], BaseDataStore):

    _archive_type_name = "sqlite_data_store"
    _config_cls = SqliteDataStoreConfig

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

        required_tables = REQUIRED_TABLES_DATA_ARCHIVE

        if not required_tables.issubset(tables):
            return None

        # config = SqliteArchiveConfig(sqlite_db_path=archive_uri)
        return {"sqlite_db_path": archive_uri}

    def _set_archive_metadata_value(self, key: str, value: Any):
        """Set custom metadata for the archive."""

        sql = text(
            f"INSERT OR REPLACE INTO {TABLE_NAME_ARCHIVE_METADATA} (key, value) VALUES (:key, :value)"
        )
        with self.sqlite_engine.connect() as conn:
            params = {"key": key, "value": value}
            conn.execute(sql, params)
            conn.commit()

    # def _persist_environment_details(
    #     self, env_type: str, env_hash: str, env_data: Mapping[str, Any]
    # ):
    #
    #     sql = text(
    #         "INSERT OR IGNORE INTO environments (environment_type, environment_hash, environment_data) VALUES (:environment_type, :environment_hash, :environment_data)"
    #     )
    #     env_data_json = orjson_dumps(env_data)
    #     with self.sqlite_engine.connect() as conn:
    #         params = {
    #             "environment_type": env_type,
    #             "environment_hash": env_hash,
    #             "environment_data": env_data_json,
    #         }
    #         conn.execute(sql, params)
    #         conn.commit()
    #     # print(env_type)
    #     # print(env_hash)
    #     # print(env_data_json)
    #     # raise NotImplementedError()

    # def _persist_value_data(self, value: Value) -> PersistedData:
    #
    #     serialized_value: SerializedData = value.serialized_data
    #     dbg(serialized_value.model_dump())
    #     dbg(serialized_value.get_keys())
    #
    #     raise NotImplementedError()

    def _persist_chunks(self, chunks: Mapping["CID", Union[str, BytesIO]]):

        all_chunk_ids = self.retrieve_all_chunk_ids()

        with self.sqlite_engine.connect() as conn:

            for chunk_id, chunk in chunks.items():
                cid_str = str(chunk_id)
                if cid_str in all_chunk_ids:
                    continue
                self._persist_chunk(conn, cid_str, chunk)

            conn.commit()

    def _persist_chunk(
        self, conn: Connection, chunk_id: str, chunk: Union[str, BytesIO]
    ):

        import lzma

        import lz4.frame
        from zstandard import ZstdCompressor

        cctx = ZstdCompressor()

        # sql = text(
        #     "SELECT EXISTS(SELECT 1 FROM values_data WHERE chunk_id = :chunk_id)"
        # )
        # with self.sqlite_engine.connect() as conn:
        #     result = conn.execute(sql, {"chunk_id": chunk_id}).scalar()
        #     if result:
        #         return

        if isinstance(chunk, str):
            with open(chunk, "rb") as file:
                file_data = file.read()
                bytes_io = BytesIO(file_data)
        else:
            bytes_io = chunk

        compression_type = CHUNK_COMPRESSION_TYPE[
            self.config.default_chunk_compression.upper()  # type: ignore
        ]

        if compression_type == CHUNK_COMPRESSION_TYPE.NONE:
            final_bytes = bytes_io.getvalue()
        elif compression_type == CHUNK_COMPRESSION_TYPE.ZSTD:
            bytes_io.seek(0)
            data = bytes_io.read()
            final_bytes = cctx.compress(data)
        elif compression_type == CHUNK_COMPRESSION_TYPE.LZMA:
            final_bytes = lzma.compress(bytes_io.getvalue())
        elif compression_type == CHUNK_COMPRESSION_TYPE.LZ4:
            bytes_io.seek(0)
            data = bytes_io.read()
            final_bytes = lz4.frame.compress(data)
        else:
            raise ValueError(
                f"Unsupported compression type: {self.config.default_chunk_compression}"
            )

        compression_type_value = (
            compression_type.value
            if compression_type is not CHUNK_COMPRESSION_TYPE.NONE
            else None
        )
        sql = text(
            f"INSERT INTO {TABLE_NAME_DATA_CHUNKS} (chunk_id, chunk_data, compression_type) VALUES (:chunk_id, :chunk_data, :compression_type)"
        )
        params = {
            "chunk_id": chunk_id,
            "chunk_data": final_bytes,
            "compression_type": compression_type_value,
        }

        conn.execute(sql, params)
        # conn.commit()

    def _persist_stored_value_info(self, value: Value, persisted_value: PersistedData):

        self._value_id_cache = None

        value_id = str(value.value_id)
        value_hash = value.value_hash
        value_size = value.value_size
        data_type_name = value.data_type_name

        metadata = persisted_value.model_dump_json()

        sql = text(
            f"INSERT INTO {TABLE_NAME_DATA_SERIALIZATION_METADATA} (value_id, value_hash, value_size, data_type_name, persisted_value_metadata) VALUES (:value_id, :value_hash, :value_size, :data_type_name, :metadata)"
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

        value_created = value.value_created.isoformat()

        metadata = value.model_dump_json()

        sql = text(
            f"INSERT INTO {TABLE_NAME_DATA_METADATA} (value_id, value_hash, value_size, value_created, data_type_name, value_metadata) VALUES (:value_id, :value_hash, :value_size, :value_created, :data_type_name, :metadata)"
        )
        with self.sqlite_engine.connect() as conn:
            params = {
                "value_id": value_id,
                "value_hash": value_hash,
                "value_size": value_size,
                "value_created": value_created,
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
                    f"INSERT INTO {TABLE_NAME_DATA_DESTINIES} (value_id, destiny_name) VALUES (:value_id, :destiny_name)"
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
            f"INSERT INTO {TABLE_NAME_DATA_PEDIGREE} (value_id, pedigree) VALUES (:value_id, :pedigree)"
        )
        with self.sqlite_engine.connect() as conn:
            params = {"value_id": value_id, "pedigree": pedigree}
            conn.execute(sql, params)
            conn.commit()
