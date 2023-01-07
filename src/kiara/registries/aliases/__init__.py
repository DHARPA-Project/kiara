# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import structlog
import uuid
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    Mapping,
    NamedTuple,
    Set,
    Union,
)

from kiara.models.events.alias_registry import AliasArchiveAddedEvent
from kiara.registries import BaseArchive
from kiara.registries.data import ValueLink

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()


class AliasArchive(BaseArchive):
    @classmethod
    def supported_item_types(cls) -> Iterable[str]:
        return ["alias"]

    @abc.abstractmethod
    def retrieve_all_aliases(self) -> Union[Mapping[str, uuid.UUID], None]:
        """Retrieve a list of all aliases registered in this archive.

        The result of this method can be 'None', for cases where the aliases are determined dynamically.
        In kiara, the result of this method is mostly used to improve performance when looking up an alias.

        Returns:
            a list of strings (the aliases), or 'None' if this archive does not support alias indexes.
        """

    @abc.abstractmethod
    def find_value_id_for_alias(self, alias: str) -> Union[uuid.UUID, None]:
        pass

    @abc.abstractmethod
    def find_aliases_for_value_id(self, value_id: uuid.UUID) -> Union[Set[str], None]:
        pass

    @classmethod
    def is_writeable(cls) -> bool:
        return False


class AliasStore(AliasArchive):
    @abc.abstractmethod
    def register_aliases(self, value_id: uuid.UUID, *aliases: str):
        pass


class AliasItem(NamedTuple):
    full_alias: str
    rel_alias: str
    value_id: uuid.UUID
    alias_archive: str
    alias_archive_id: uuid.UUID


class AliasRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

        self._event_callback: Callable = self._kiara.event_registry.add_producer(self)

        self._alias_archives: Dict[str, AliasArchive] = {}
        """All registered archives/stores."""

        self._default_alias_store: Union[str, None] = None
        """The alias of the store where new aliases are stored by default."""

        self._cached_aliases: Union[Dict[str, AliasItem], None] = None
        self._cached_aliases_by_id: Union[Dict[uuid.UUID, Set[AliasItem]], None] = None

    def register_archive(
        self,
        archive: AliasArchive,
        alias: Union[str, None] = None,
        set_as_default_store: Union[bool, None] = None,
    ):

        alias_archive_id = archive.archive_id
        archive.register_archive(kiara=self._kiara)

        if alias is None:
            alias = str(alias_archive_id)

        if "." in alias:
            raise Exception(
                f"Can't register alias archive with as '{alias}': registered name is not allowed to contain a '.' character (yet)."
            )

        if alias in self._alias_archives.keys():
            raise Exception(f"Can't add store, alias '{alias}' already registered.")

        self._alias_archives[alias] = archive
        is_store = False
        is_default_store = False
        if isinstance(archive, AliasStore):
            is_store = True
            if set_as_default_store and self._default_alias_store is not None:
                raise Exception(
                    f"Can't set alias store '{alias}' as default store: default store already set."
                )

            if self._default_alias_store is None:
                is_default_store = True
                self._default_alias_store = alias

        event = AliasArchiveAddedEvent.construct(
            kiara_id=self._kiara.id,
            alias_archive_id=archive.archive_id,
            alias_archive_alias=alias,
            is_store=is_store,
            is_default_store=is_default_store,
        )
        self._event_callback(event)

    @property
    def default_alias_store(self) -> str:

        if self._default_alias_store is None:
            raise Exception("No default alias store set (yet).")
        return self._default_alias_store

    @property
    def alias_archives(self) -> Mapping[str, AliasArchive]:
        return self._alias_archives

    def get_archive(
        self, archive_id: Union[str, None] = None
    ) -> Union[AliasArchive, None]:
        if archive_id is None:
            archive_id = self.default_alias_store
            if archive_id is None:
                raise Exception("Can't retrieve default alias archive, none set (yet).")

        archive = self._alias_archives.get(archive_id, None)
        return archive

    @property
    def all_aliases(self) -> Iterable[str]:

        return self.aliases.keys()

    @property
    def aliases_by_id(self) -> Mapping[uuid.UUID, Set[AliasItem]]:
        if self._cached_aliases_by_id is None:
            self.aliases  # noqa
        return self._cached_aliases_by_id  # type: ignore

    @property
    def aliases(self) -> Dict[str, AliasItem]:
        """Retrieve a map of all available aliases, context wide, with the registered archive aliases as values."""

        if self._cached_aliases is not None:
            return self._cached_aliases

        # TODO: multithreading lock

        all_aliases: Dict[str, AliasItem] = {}
        all_aliases_by_id: Dict[uuid.UUID, Set[AliasItem]] = {}
        for archive_alias, archive in self._alias_archives.items():
            alias_map = archive.retrieve_all_aliases()
            if alias_map is None:
                continue
            for alias, v_id in alias_map.items():
                if archive_alias == self.default_alias_store:
                    final_alias = alias
                else:
                    final_alias = f"{archive_alias}.{alias}"

                if final_alias in all_aliases.keys():
                    raise Exception(
                        f"Inconsistent alias registry: alias '{final_alias}' available more than once."
                    )
                item = AliasItem(
                    full_alias=final_alias,
                    rel_alias=alias,
                    value_id=v_id,
                    alias_archive=archive_alias,
                    alias_archive_id=archive.archive_id,
                )
                all_aliases[final_alias] = item
                all_aliases_by_id.setdefault(v_id, set()).add(item)

        self._cached_aliases = {k: all_aliases[k] for k in sorted(all_aliases.keys())}
        self._cached_aliases_by_id = all_aliases_by_id
        return self._cached_aliases

    def find_value_id_for_alias(self, alias: str) -> Union[uuid.UUID, None]:

        alias_item = self.aliases.get(alias, None)
        if alias_item is not None:
            return alias_item.value_id

        if "." not in alias:
            return None

        archive_id, rest = alias.split(".", maxsplit=2)
        archive = self.get_archive(archive_id=archive_id)

        if archive is None:
            # means no registered prefix
            archive = self.get_archive()
            assert archive is not None
            v_id = archive.find_value_id_for_alias(alias)
        else:
            v_id = archive.find_value_id_for_alias(alias=rest)

        # TODO: cache this?
        return v_id

    def _get_value_id(self, value_id: Union[uuid.UUID, ValueLink, str]) -> uuid.UUID:

        if not isinstance(value_id, uuid.UUID):
            # fallbacks for common mistakes, this should error out if not a Value or string.
            if hasattr(value_id, "value_id"):
                _value_id: Union[uuid.UUID, str] = value_id.value_id  # type: ignore
                if isinstance(_value_id, str):
                    _value_id = uuid.UUID(_value_id)
            else:
                _value_id = uuid.UUID(
                    value_id  # type: ignore
                )  # this should fail if not string or wrong string format
        else:
            _value_id = value_id

        if not _value_id:
            raise Exception(f"Could not resolve id: {value_id}")
        return _value_id

    def find_aliases_for_value_id(
        self,
        value_id: Union[uuid.UUID, ValueLink, str],
        search_dynamic_archives: bool = False,
    ) -> Set[str]:

        value_id = self._get_value_id(value_id=value_id)

        aliases = set([a.full_alias for a in self.aliases_by_id.get(value_id, [])])

        if search_dynamic_archives:
            for archive_alias, archive in self._alias_archives.items():
                _aliases = archive.find_aliases_for_value_id(value_id=value_id)
                # TODO: cache those results
                if _aliases:
                    for a in _aliases:
                        aliases.add(f"{archive_alias}.{a}")

        return aliases

    def register_aliases(
        self,
        value_id: Union[uuid.UUID, ValueLink, str],
        *aliases: str,
        allow_overwrite: bool = False,
    ):

        value_id = self._get_value_id(value_id=value_id)
        store_name = self.default_alias_store
        store: AliasStore = self.get_archive(archive_id=store_name)  # type: ignore
        self.aliases  # noqu

        if not allow_overwrite:
            duplicates = []
            for alias in aliases:
                if alias in self.aliases.keys():
                    duplicates.append(alias)

            if duplicates:
                raise Exception(f"Duplicate aliases: {duplicates}")

        store.register_aliases(value_id, *aliases)

        for alias in aliases:
            alias_item = AliasItem(
                full_alias=alias,
                rel_alias=alias,
                value_id=value_id,
                alias_archive=store_name,
                alias_archive_id=store.archive_id,
            )

            if alias in self.aliases.keys():
                logger.info("alias.replace", alias=alias)
                # raise NotImplementedError()

            self.aliases[alias] = alias_item
            self._cached_aliases_by_id.setdefault(value_id, set()).add(alias_item)  # type: ignore


#
# class PersistentValueAliasMap(AliasValueMap):
#     # def __init__(self, data_registry: "DataRegistry", engine: Engine, doc: Any = None):
#     #
#     #     self._data_registry: DataRegistry = data_registry
#     #     self._engine: Engine = engine
#     #     doc = DocumentationMetadataModel.create(doc)
#     #     v_doc = self._data_registry.register_data(
#     #         doc, schema=ValueSchema(type="doc"), pedigree=ORPHAN
#     #     )
#     #     super().__init__(alias="", version=0, value=v_doc)
#     #
#     #     self._load_all_aliases()
#     doc: Optional[DocumentationMetadataModel] = Field(
#         description="Description of the values this map contains."
#     )
#     _engine: Engine = PrivateAttr(default=None)
#
#     @root_validator(pre=True)
#     def _fill_defaults(cls, values):
#         if "values_schema" not in values.keys():
#             values["values_schema"] = {}
#
#         if "version" not in values.keys():
#             values["version"] = 0
#         else:
#             assert values["version"] == 0
#
#         return values
#
#     def _load_all_aliases(self):
#
#         with Session(bind=self._engine, future=True) as session:  # type: ignore
#
#             alias_a = aliased(AliasOrm)
#             alias_b = aliased(AliasOrm)
#
#             result = (
#                 session.query(alias_b)
#                 .join(
#                     alias_a,
#                     and_(
#                         alias_a.alias == alias_b.alias,
#                         alias_a.version < alias_b.version,
#                     ),
#                 )
#                 .where(alias_b.value_id != None)
#                 .order_by(func.length(alias_b.alias), alias_b.alias)
#             )
#
#             for r in result:
#                 value = self._data_registry.get_value(r.value_id)
#                 self.set_alias(r.alias, value=value)
#
#     def save(self, *aliases):
#
#         for alias in aliases:
#             self._persist(alias)
#
#     def _persist(self, alias: str):
#
#         return
#
#         with Session(bind=self._engine, future=True) as session:  # type: ignore
#
#             current = []
#             tokens = alias.split(".")
#             for token in tokens:
#                 current.append(token)
#                 current_path = ".".join(current)
#                 alias_map = self.get_alias(current_path)
#                 if alias_map.is_stored:
#                     continue
#
#                 value_id = None
#                 if alias_map.assoc_value:
#                     value_id = alias_map.assoc_value
#
#                 if value_id is None:
#                     continue
#                 alias_map_orm = AliasOrm(
#                     value_id=value_id,
#                     created=alias_map.created,
#                     version=alias_map.version,
#                     alias=current_path,
#                 )
#                 session.add(alias_map_orm)
#
#             session.commit()
