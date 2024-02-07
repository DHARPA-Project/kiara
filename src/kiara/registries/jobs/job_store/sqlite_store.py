# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Union

import orjson
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from kiara.models.module.jobs import JobRecord
from kiara.registries import SqliteArchiveConfig
from kiara.registries.jobs import JobArchive, JobStore
from kiara.utils.windows import fix_windows_longpath


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

        required_tables = {
            "job_records",
        }

        if not required_tables.issubset(tables):
            return None

        # config = SqliteArchiveConfig(sqlite_db_path=store_uri)
        return {"sqlite_db_path": archive_uri}

    def __init__(
        self,
        archive_alias: str,
        archive_config: SqliteArchiveConfig,
        force_read_only: bool = False,
    ):

        super().__init__(
            archive_alias=archive_alias,
            archive_config=archive_config,
            force_read_only=force_read_only,
        )
        self._db_path: Union[Path, None] = None
        self._cached_engine: Union[Engine, None] = None
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
CREATE TABLE IF NOT EXISTS job_records (
    job_hash TEXT PRIMARY KEY,
    manifest_hash TEXT NOT NULL,
    inputs_hash TEXT NOT NULL,
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

        sql = text("SELECT job_metadata FROM job_records WHERE job_hash = :job_hash")
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

    def retrieve_all_job_hashes(
        self,
        manifest_hash: Union[str, None] = None,
        inputs_hash: Union[str, None] = None,
    ) -> Iterable[str]:

        if not manifest_hash:
            if not inputs_hash:
                sql = text("SELECT job_hash FROM job_records")
                params = {}
            else:
                sql = text(
                    "SELECT job_hash FROM job_records WHERE inputs_hash = :inputs_hash"
                )
                params = {"inputs_hash": inputs_hash}
        else:
            if not inputs_hash:
                sql = text(
                    "SELECT job_hash FROM job_records WHERE manifest_hash = :manifest_hash"
                )
                params = {"manifest_hash": manifest_hash}
            else:
                sql = text(
                    "SELECT job_hash FROM job_records WHERE manifest_hash = :manifest_hash AND inputs_hash = :inputs_hash"
                )
                params = {"manifest_hash": manifest_hash, "inputs_hash": inputs_hash}

        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql, params)
            return {row[0] for row in result}


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

        required_tables = {
            "job_records",
        }

        if not required_tables.issubset(tables):
            return None

        # config = SqliteArchiveConfig(sqlite_db_path=store_uri)
        return {"sqlite_db_path": archive_uri}

    def store_job_record(self, job_record: JobRecord):

        manifest_hash = str(job_record.manifest_cid)
        inputs_hash = job_record.inputs_hash

        job_hash = job_record.job_hash
        job_record_json = job_record.model_dump_json()

        sql = text(
            "INSERT INTO job_records (job_hash, manifest_hash, inputs_hash, job_metadata) VALUES (:job_hash, :manifest_hash, :inputs_hash, :job_metadata)"
        )
        params = {
            "job_hash": job_hash,
            "manifest_hash": manifest_hash,
            "inputs_hash": inputs_hash,
            "job_metadata": job_record_json,
        }

        with self.sqlite_engine.connect() as connection:
            connection.execute(sql, params)

            connection.commit()
