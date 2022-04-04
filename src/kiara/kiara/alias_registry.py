# -*- coding: utf-8 -*-
import abc
import os
import uuid
from pathlib import Path

import structlog
from pydantic import Field, PrivateAttr, root_validator
from sqlalchemy import and_, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, aliased
from typing import Optional, TYPE_CHECKING, Dict, Iterable, List

from kiara.models.aliases import AliasValueMap
from kiara.models.documentation import DocumentationMetadataModel
from kiara.kiara.id_registry import ID_REGISTRY

if TYPE_CHECKING:
    from kiara.kiara import Kiara

logger = structlog.getLogger()


class AliasArchive(abc.ABC):

    @abc.abstractmethod
    def get_alias_archive_id(self) -> uuid.UUID:
        pass

    @abc.abstractmethod
    def retrieve_all_aliases(self) -> Optional[Iterable[str]]:
        """Retrieve a list of all aliases registered in this archive.

        The result of this method can be 'None', for cases where the aliases are determined dynamically.
        In kiara, the result of this method is mostly used to improve performance when looking up an alias.

        Returns:
            a list of strings (the aliases), or 'None' if this archive does not support alias indexes.
        """
        pass

    @abc.abstractmethod
    def find_value_id_for_alias(self, alias: str) -> Optional[uuid.UUID]:
        pass

class AliasStore(AliasArchive):

    @abc.abstractmethod
    def register_aliases(self, value_id: uuid.UUID, *aliases: str):
        pass

class FileSystemAliasArchive(AliasArchive):

    @classmethod
    def create_from_kiara_context(cls, kiara: "Kiara"):

        base_path = Path(kiara.context_config.data_directory) / "alias_store"
        base_path.mkdir(parents=True, exist_ok=True)
        result = cls(base_path=base_path, store_id=kiara.id)
        ID_REGISTRY.update_metadata(result.get_alias_archive_id(), kiara_id=kiara.id, obj=result)
        return result

    def __init__(self, base_path: Path, store_id: uuid.UUID):

        if not base_path.is_dir():
            raise Exception(
                f"Can't create file system archive instance, base path does not exist or is not a folder: {base_path.as_posix()}."
            )

        self._store_id: uuid.UUID = store_id
        self._base_path: Path = base_path
        self._aliases_path: Path = self._base_path / "aliases"
        self._value_id_path: Path = self._base_path / "value_ids"

    def _translate_alias(self, alias: str) -> Path:

        if "." in alias:
            tokens = alias.split(".")
            alias_path = self._aliases_path.joinpath(*tokens[0:-1]) / f"{tokens[-1]}.alias"
        else:
            alias_path = self._aliases_path / f"{alias}.alias"
        return alias_path

    def _translate_alias_path(self, alias_path: Path) -> str:

        relative = alias_path.relative_to(self._aliases_path).as_posix()[:-6]

        if os.path.sep not in relative:
            alias = relative
        else:
            alias = ".".join(relative.split(os.path.sep))

        return alias

    def _translate_value_id(self, value_id: uuid.UUID) -> Path:

        tokens = str(value_id).split("-")
        value_id_path = self._value_id_path.joinpath(*tokens[0:-1]) / f"{tokens[-1]}.value"
        return value_id_path

    def _translate_value_path(self, value_path: Path) -> uuid.UUID:

        relative = value_path.relative_to(self._value_id_path).as_posix()[:-6]
        value_id_str = "-".join(relative.split(os.path.sep))

        return uuid.UUID(value_id_str)

    def get_alias_archive_id(self) -> uuid.UUID:
        return self._store_id

    def retrieve_all_aliases(self) -> Iterable[str]:

        all_aliases = self._aliases_path.rglob("*.alias")
        result = []
        for alias_path in all_aliases:
            alias = self._translate_alias_path(alias_path=alias_path)
            result.append(alias)

        return sorted(result)

    def find_value_id_for_alias(self, alias: str) -> Optional[uuid.UUID]:

        alias_path = self._translate_alias(alias)
        if not alias_path.exists():
            return None
        resolved = alias_path.resolve()

        assert resolved.name.endswith(".value")

        value_id = self._translate_value_path(value_path=resolved)
        return value_id

class FileSystemAliasStore(FileSystemAliasArchive, AliasStore):

    def register_aliases(self, value_id: uuid.UUID, *aliases: str):

        value_path = self._translate_value_id(value_id=value_id)
        value_path.parent.mkdir(parents=True, exist_ok=True)
        value_path.touch()

        for alias in aliases:
            alias_path = self._translate_alias(alias)
            alias_path.parent.mkdir(parents=True, exist_ok=True)
            if alias_path.exists():
                resolved = alias_path.resolve()
                if resolved == value_path:
                    continue
                alias_path.unlink()
            alias_path.symlink_to(value_path)


class AliasRegistry(object):

    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._alias_archives: Dict[uuid.UUID, AliasArchive] = {}
        self._default_alias_store: Optional[AliasStore] = None
        default_archive = FileSystemAliasStore.create_from_kiara_context(self._kiara)
        self.register_alias_archive(
            default_archive
        )

        self._cached_aliases: Dict[str, uuid.UUID] = {}
        self._all_aliases: Optional[Dict[str, uuid.UUID]] = None

    def register_alias_archive(self, alias_store: AliasStore):

        as_id = alias_store.get_alias_archive_id()
        if as_id in self._alias_archives.keys():
            raise Exception(
                f"Can't register alias store, store id already registered: {as_id}."
            )

        self._alias_archives[as_id] = alias_store

        if self._default_alias_store is None and isinstance(alias_store, AliasStore):
            self._default_alias_store = alias_store

    @property
    def default_alias_store(self) -> AliasStore:

        if self._default_alias_store is None:
            raise Exception("No default alias store set (yet).")
        return self._default_alias_store

    @property
    def all_aliases(self) -> Iterable[str]:

        if self._all_aliases is not None:
            return self._all_aliases

        # TODO: multithreading lock

        all_aliases: Dict[str, uuid.UUID] = {}
        for store_id, store in self._alias_archives.items():
            a = store.retrieve_all_aliases()
            if a is None:
                continue
            for alias in a:
                if alias in all_aliases.keys():
                    value_one = self._alias_archives[all_aliases[alias]].find_value_id_for_alias(alias)
                    value_two = store.find_value_id_for_alias(alias)
                    if value_one == value_two:
                        self._cached_aliases[alias] = value_one
                        continue
                    else:
                        raise Exception(f"Multiple stores contain alias '{alias}'. This is not supported (yet).")
                all_aliases[alias] = store_id

        self._all_aliases = sorted(all_aliases)
        return self._all_aliases

    def find_value_id_for_alias(self, alias: str) -> uuid.UUID:

        value_id = self._cached_aliases.get(alias, None)
        if value_id is not None:
            return self._cached_aliases[alias]

        for alias_archive in self._alias_archives.values():
            value_id = alias_archive.find_value_id_for_alias(alias=alias)
            if value_id is not None:
                break

        if value_id is None:
            return None

        self._cached_aliases[alias] = value_id
        return value_id

    def register_aliases(self, value_id: uuid.UUID, *aliases: str):

        self.default_alias_store.register_aliases(value_id, *aliases)
        for alias in aliases:
            self._cached_aliases[alias] = value_id
            if alias not in self.all_aliases:
                self.all_aliases[alias] = self.default_alias_store.get_alias_archive_id()


class PersistentValueAliasMap(AliasValueMap):
    # def __init__(self, data_registry: "DataRegistry", engine: Engine, doc: Any = None):
    #
    #     self._data_registry: DataRegistry = data_registry
    #     self._engine: Engine = engine
    #     doc = DocumentationMetadataModel.create(doc)
    #     v_doc = self._data_registry.register_data(
    #         doc, schema=ValueSchema(type="doc"), pedigree=ORPHAN
    #     )
    #     super().__init__(alias="", version=0, value=v_doc)
    #
    #     self._load_all_aliases()
    doc: Optional[DocumentationMetadataModel] = Field(
        description="Description of the values this map contains."
    )
    _engine: Engine = PrivateAttr(default=None)

    @root_validator(pre=True)
    def _fill_defaults(cls, values):
        if "values_schema" not in values.keys():
            values["values_schema"] = {}

        if "version" not in values.keys():
            values["version"] = 0
        else:
            assert values["version"] == 0

        return values

    def _load_all_aliases(self):

        with Session(bind=self._engine, future=True) as session:  # type: ignore

            alias_a = aliased(AliasOrm)
            alias_b = aliased(AliasOrm)

            result = (
                session.query(alias_b)
                .join(
                    alias_a,
                    and_(
                        alias_a.alias == alias_b.alias,
                        alias_a.version < alias_b.version,
                    ),
                )
                .where(alias_b.value_id != None)
                .order_by(func.length(alias_b.alias), alias_b.alias)
            )

            for r in result:
                value = self._data_registry.get_value(r.value_id)
                self.set_alias(r.alias, value=value)

    def save(self, *aliases):

        for alias in aliases:
            self._persist(alias)

    def _persist(self, alias: str):

        return

        with Session(bind=self._engine, future=True) as session:  # type: ignore

            current = []
            tokens = alias.split(".")
            for token in tokens:
                current.append(token)
                current_path = ".".join(current)
                alias_map = self.get_alias(current_path)
                if alias_map.is_stored:
                    continue

                value_id = None
                if alias_map.assoc_value:
                    value_id = alias_map.assoc_value

                if value_id is None:
                    continue
                alias_map_orm = AliasOrm(
                    value_id=value_id,
                    created=alias_map.created,
                    version=alias_map.version,
                    alias=current_path,
                )
                session.add(alias_map_orm)

            session.commit()
