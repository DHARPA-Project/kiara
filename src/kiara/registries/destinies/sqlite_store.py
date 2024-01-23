# -*- coding: utf-8 -*-
import uuid
from pathlib import Path
from typing import Set, Union

from sqlalchemy import Engine, create_engine, text

from kiara.models.module.destiny import Destiny
from kiara.registries import SqliteArchiveConfig
from kiara.registries.destinies import DestinyArchive, DestinyStore
from kiara.utils.windows import fix_windows_longpath


class SqliteDestinyArchive(DestinyArchive):

    _archive_type_name = "sqlite_destiny_archive"
    _config_cls = SqliteArchiveConfig

    def __init__(self, archive_alias: str, archive_config: SqliteArchiveConfig):

        DestinyArchive.__init__(
            self, archive_alias=archive_alias, archive_config=archive_config
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
CREATE TABLE IF NOT EXISTS destiny_details (
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

    def get_all_value_ids(self) -> Set[uuid.UUID]:
        pass

    def get_destiny_aliases_for_value(
        self, value_id: uuid.UUID
    ) -> Union[Set[str], None]:
        pass

    def get_destiny(self, value_id: uuid.UUID, destiny: str) -> Destiny:
        pass


class SqliteJobStore(SqliteDestinyArchive, DestinyStore):

    _archive_type_name = "sqlite_destiny_store"

    def persist_destiny(self, destiny: Destiny):

        for value_id in destiny.fixed_inputs.values():

            path = self._translate_value_id(
                value_id=value_id, destiny_alias=destiny.destiny_alias
            )
            if path.exists():
                logger.debug("replace.destiny.file", path=path.as_posix())
                path.unlink()
                # raise Exception(
                #     f"Can't persist destiny '{destiny.destiny_id}': already persisted."
                # )

            path.parent.mkdir(parents=True, exist_ok=True)
            fix_windows_symlink(destiny_path, path)
