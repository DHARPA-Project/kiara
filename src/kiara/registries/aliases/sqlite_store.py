# -*- coding: utf-8 -*-
import uuid
from pathlib import Path
from typing import Mapping, Set, Union

from sqlalchemy import Engine, create_engine, text

from kiara.registries import SqliteArchiveConfig
from kiara.registries.aliases import AliasArchive, AliasStore
from kiara.utils.windows import fix_windows_longpath


class SqliteAliasArchive(AliasArchive):

    _archive_type_name = "sqlite_alias_archive"
    _config_cls = SqliteArchiveConfig

    def __init__(self, archive_id: uuid.UUID, config: SqliteArchiveConfig):

        AliasArchive.__init__(self, archive_id=archive_id, config=config)
        self._db_path: Union[Path, None] = None
        self._cached_engine: Union[Engine, None] = None
        # self._lock: bool = True

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
CREATE TABLE IF NOT EXISTS aliases (
    alias TEXT PRIMARY KEY,
    value_id TEXT NOT NULL
);
"""

        with self._cached_engine.begin() as connection:
            for statement in create_table_sql.split(";"):
                if statement.strip():
                    connection.execute(text(statement))

        # if self._lock:
        #     event.listen(self._cached_engine, "connect", _pragma_on_connect)
        return self._cached_engine

    def find_value_id_for_alias(self, alias: str) -> Union[uuid.UUID, None]:

        sql = text("SELECT value_id FROM aliases WHERE alias = :alias")
        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql, {"alias": alias})
            row = result.fetchone()
            if row is None:
                return None
            return uuid.UUID(row[0])

    def find_aliases_for_value_id(self, value_id: uuid.UUID) -> Union[Set[str], None]:

        sql = text("SELECT alias FROM aliases WHERE value_id = :value_id")
        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql, {"value_id": str(value_id)})
            return {row[0] for row in result}

    def retrieve_all_aliases(self) -> Union[Mapping[str, uuid.UUID], None]:

        sql = text("SELECT alias, value_id FROM aliases")
        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sql)
            return {row[0]: uuid.UUID(row[1]) for row in result}


class SqliteAliasStore(SqliteAliasArchive, AliasStore):

    _archive_type_name = "sqlite_alias_store"

    def register_aliases(self, value_id: uuid.UUID, *aliases: str):

        sql = text(
            "INSERT OR REPLACE INTO aliases (alias, value_id) VALUES (:alias, :value_id)"
        )

        with self.sqlite_engine.connect() as connection:
            params = [{"alias": alias, "value_id": str(value_id)} for alias in aliases]

            for param in params:
                connection.execute(sql, param)

            connection.commit()
