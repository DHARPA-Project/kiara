# -*- coding: utf-8 -*-
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Mapping, Union

import orjson
from sqlalchemy import text
from sqlalchemy.engine import Engine

from kiara.defaults import (
    REQUIRED_TABLES_JOB_ARCHIVE,
    TABLE_NAME_ARCHIVE_METADATA,
    TABLE_NAME_JOB_RECORDS,
)
from kiara.models.module.jobs import JobMatcher, JobRecord
from kiara.registries import ArchiveDetails, SqliteArchiveConfig
from kiara.registries.jobs import JobArchive, JobStore
from kiara.utils.db import create_archive_engine, delete_archive_db


class SqliteJobArchive(JobArchive):

    _archive_type_name = "sqlite_job_archive"
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

        required_tables = REQUIRED_TABLES_JOB_ARCHIVE

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

        sql = text(f"SELECT key, value FROM {TABLE_NAME_ARCHIVE_METADATA}")

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

        create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME_JOB_RECORDS} (
    job_id TEXT PRIMARY KEY,
    job_hash TEXT TEXT NOT NULL,
    job_submitted TEXT NOT NULL,
    manifest_hash TEXT NOT NULL,
    input_ids_hash TEXT NOT NULL,
    inputs_data_hash TEXT NOT NULL,
    job_metadata TEXT NOT NULL
);
"""

        with self._cached_engine.begin() as connection:
            for statement in create_table_sql.split(";"):
                if statement.strip():
                    connection.execute(text(statement))

        # if self._lock:
        #     event.listen(self._cached_engine, "connect", _pragma_on_connect)
        return self._cached_engine

    def _retrieve_record_for_job_hash(self, job_hash: str) -> Union[JobRecord, None]:

        sql = text(
            f"SELECT job_metadata FROM {TABLE_NAME_JOB_RECORDS} WHERE job_hash = :job_hash"
        )
        params = {"job_hash": job_hash}

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql, params)
            row = result.fetchone()
            if not row:
                return None

            job_record_json = row[0]
            job_record_data = orjson.loads(job_record_json)
            job_record = JobRecord(**job_record_data)
            return job_record

    def _retrieve_all_job_ids(self) -> Mapping[uuid.UUID, datetime]:
        """
        Retrieve a list of all job record ids in the archive.
        """

        sql = text(
            f"SELECT job_id, job_submitted FROM {TABLE_NAME_JOB_RECORDS} ORDER BY job_submitted DESC;"
        )

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql)
            return {uuid.UUID(row[0]): datetime.fromisoformat(row[1]) for row in result}

    def _retrieve_record_for_job_id(self, job_id: uuid.UUID) -> Union[JobRecord, None]:

        sql = text(
            f"SELECT job_metadata FROM {TABLE_NAME_JOB_RECORDS} WHERE job_id = :job_id"
        )

        params = {"job_id": str(job_id)}

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql, params)
            row = result.fetchone()
            if not row:
                return None

            job_record_json = row[0]
            job_record_data = orjson.loads(job_record_json)
            job_record = JobRecord(**job_record_data)
            return job_record

    def _retrieve_matching_job_records(
        self, matcher: JobMatcher
    ) -> Generator[JobRecord, None, None]:

        query_conditions = []
        params: Dict[str, Any] = {}
        if matcher.job_ids:
            query_conditions.append("job_id IN :job_ids")
            params["job_ids"] = (str(x) for x in matcher.job_ids)

        if not matcher.allow_internal:
            cond = "json_extract(job_metadata, '$.is_internal') = 0"
            query_conditions.append(cond)

        if matcher.earliest:
            cond = "job_submitted >= :earliest"
            query_conditions.append(cond)
            params["earliest"] = matcher.earliest.isoformat()

        if matcher.latest:
            cond = "job_submitted <= :latest"
            query_conditions.append(cond)
            params["latest"] = matcher.latest.isoformat()

        if matcher.operation_inputs:
            raise NotImplementedError(
                "Job matcher 'operation_inputs' not implemented yet"
            )

        if matcher.produced_outputs:
            raise NotImplementedError(
                "Job matcher 'produced_outputs' not implemented yet"
            )

        sql_query = f"SELECT job_id, job_metadata FROM {TABLE_NAME_JOB_RECORDS}"
        if query_conditions:
            sql_query += " WHERE "

            for query_cond in query_conditions:
                sql_query += "( " + query_cond + " ) AND "

            sql_query = sql_query[:-5] + ";"

        sql = text(sql_query)

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql, params)
            for row in result:
                # job_id = uuid.UUID(row[0])
                job_metadata = orjson.loads(row[1])
                job_record = JobRecord(**job_metadata)
                yield job_record

        return

    def retrieve_all_job_hashes(
        self,
        manifest_hash: Union[str, None] = None,
        inputs_id_hash: Union[str, None] = None,
        inputs_data_hash: Union[str, None] = None,
    ) -> Iterable[str]:

        if not manifest_hash:
            if not inputs_id_hash:
                sql = text(f"SELECT job_hash FROM {TABLE_NAME_JOB_RECORDS}")
                params = {}
            else:
                sql = text(
                    f"SELECT job_hash FROM {TABLE_NAME_JOB_RECORDS} WHERE inputs_hash = :inputs_hash"
                )
                params = {"inputs_hash": inputs_id_hash}
        else:
            if not inputs_id_hash:
                sql = text(
                    f"SELECT job_hash FROM {TABLE_NAME_JOB_RECORDS} WHERE manifest_hash = :manifest_hash"
                )
                params = {"manifest_hash": manifest_hash}
            else:
                sql = text(
                    f"SELECT job_hash FROM {TABLE_NAME_JOB_RECORDS} WHERE manifest_hash = :manifest_hash AND inputs_hash = :inputs_hash"
                )
                params = {"manifest_hash": manifest_hash, "inputs_hash": inputs_id_hash}

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql, params)
            return {row[0] for row in result}

    def _delete_archive(self):

        delete_archive_db(db_path=self.sqlite_path)

    def get_archive_details(self) -> ArchiveDetails:

        all_job_records_sql = text(f"SELECT COUNT(*) FROM {TABLE_NAME_JOB_RECORDS}")

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(all_job_records_sql)
            job_count = result.fetchone()[0]

            details = {"no_job_records": job_count, "dynamic_archive": False}
            return ArchiveDetails(**details)


class SqliteJobStore(SqliteJobArchive, JobStore):

    _archive_type_name = "sqlite_job_store"

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

        required_tables = REQUIRED_TABLES_JOB_ARCHIVE

        if not required_tables.issubset(tables):
            return None

        # config = SqliteArchiveConfig(sqlite_db_path=store_uri)
        return {"sqlite_db_path": archive_uri}

    def store_job_record(self, job_record: JobRecord):

        job_hash = job_record.job_hash
        manifest_hash = job_record.manifest_hash
        input_ids_hash = job_record.input_ids_hash
        inputs_data_hash = job_record.inputs_data_hash

        job_record_json = job_record.model_dump_json()

        job_submitted = job_record.job_submitted.isoformat()

        sql = text(
            f"INSERT OR IGNORE INTO {TABLE_NAME_JOB_RECORDS}(job_id, job_submitted, job_hash, manifest_hash, input_ids_hash, inputs_data_hash, job_metadata) VALUES (:job_id, :job_submitted, :job_hash, :manifest_hash, :input_ids_hash, :inputs_data_hash, :job_metadata)"
        )
        params = {
            "job_id": str(job_record.job_id),
            "job_submitted": job_submitted,
            "job_hash": job_hash,
            "manifest_hash": manifest_hash,
            "input_ids_hash": input_ids_hash,
            "inputs_data_hash": inputs_data_hash,
            "job_metadata": job_record_json,
        }

        with self.sqlite_engine.connect() as connection:
            connection.execute(sql, params)

            connection.commit()

    def _set_archive_metadata_value(self, key: str, value: Any):
        """Set custom metadata for the archive."""

        sql = text(
            f"INSERT OR REPLACE INTO {TABLE_NAME_ARCHIVE_METADATA} (key, value) VALUES (:key, :value)"
        )
        with self.sqlite_engine.connect() as conn:
            params = {"key": key, "value": value}
            conn.execute(sql, params)
            conn.commit()
