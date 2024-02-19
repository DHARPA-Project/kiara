# -*- coding: utf-8 -*-
import uuid
from pathlib import Path
from typing import Iterable, Mapping, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from kiara.models.workflow import WorkflowMetadata, WorkflowState
from kiara.registries import SqliteArchiveConfig
from kiara.registries.workflows import WorkflowArchive, WorkflowStore
from kiara.utils.windows import fix_windows_longpath


class SqliteWorkflowArchive(WorkflowArchive[SqliteArchiveConfig]):

    _archive_type_name = "sqlite_workflow_archive"
    _config_cls = SqliteArchiveConfig

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

    def retrieve_all_workflow_aliases(self) -> Mapping[str, uuid.UUID]:

        raise NotImplementedError()

    def retrieve_all_workflow_ids(self) -> Iterable[uuid.UUID]:

        raise NotImplementedError()

    def retrieve_workflow_metadata(self, workflow_id: uuid.UUID) -> WorkflowMetadata:

        raise NotImplementedError()

    def retrieve_workflow_state(self, workflow_state_id: str) -> WorkflowState:

        raise NotImplementedError()

    def retrieve_all_states_for_workflow(
        self, workflow_id: uuid.UUID
    ) -> Mapping[str, WorkflowState]:

        raise NotImplementedError()


class SqliteWorkflowStore(SqliteWorkflowArchive, WorkflowStore):

    _archive_type_name = "sqlite_workflow_store"

    def _register_workflow_metadata(self, workflow_metadata: WorkflowMetadata) -> None:

        raise NotImplementedError()

    def _update_workflow_metadata(self, workflow_metadata: WorkflowMetadata):

        raise NotImplementedError()

    def add_workflow_state(self, workflow_state: WorkflowState):

        raise NotImplementedError()

    def register_alias(self, workflow_id: uuid.UUID, alias: str):

        raise NotImplementedError()
