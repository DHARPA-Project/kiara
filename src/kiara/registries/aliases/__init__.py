# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import uuid
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Set,
    Union,
)

import structlog

from kiara.defaults import (
    DEFAULT_ALIAS_STORE_MARKER,
    DEFAULT_STORE_MARKER,
    INVALID_ALIAS_NAMES,
)
from kiara.exceptions import KiaraException
from kiara.models.events.alias_registry import AliasArchiveAddedEvent
from kiara.registries import ArchiveDetails, BaseArchive
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
        """
        Retrieve a list of all aliases registered in this archive.

        The result of this method can be 'None', for cases where the aliases are determined dynamically.
        In kiara, the result of this method is mostly used to improve performance when looking up an alias.

        Returns:
        -------
            a list of strings (the aliases), or 'None' if this archive does not support alias indexes.
        """

    @abc.abstractmethod
    def find_value_id_for_alias(self, alias: str) -> Union[uuid.UUID, None]:
        pass

    @abc.abstractmethod
    def find_aliases_for_value_id(self, value_id: uuid.UUID) -> Union[Set[str], None]:
        pass

    def get_archive_details(self) -> ArchiveDetails:
        all_aliases = self.retrieve_all_aliases()
        if all_aliases is not None:
            no_aliases = len(all_aliases)
            aliases = sorted(all_aliases.keys())
            details = {
                "no_aliases": no_aliases,
                "aliases": aliases,
                "dynamic_archive": False,
            }
        else:
            details = {"dynamic_archive": True}
        return ArchiveDetails(root=details)


class AliasStore(AliasArchive):
    @abc.abstractmethod
    def register_aliases(self, value_id: uuid.UUID, *aliases: str):
        pass

    @classmethod
    def _is_writeable(cls) -> bool:
        return True


class AliasItem(NamedTuple):
    full_alias: str
    rel_alias: str
    value_id: uuid.UUID
    alias_archive: str
    alias_archive_id: uuid.UUID


class AliasRegistry(object):
    """The registry that handles all alias-related operations.

    This registry is responsible for managing all alias archives and stores, and for providing a unified view of all
    of them.

    Aliase archives/stores can be 'mounted' at specific mountpoints, and aliases refering to them use the format

    <mountpoint>#<actual_alias>

    There is also a 'default' alias store, which is used when the alias provided does not contain a '#' indicating a
     mountpoint.
    """

    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

        self._event_callback: Callable = self._kiara.event_registry.add_producer(self)

        self._alias_archives: Dict[str, AliasArchive] = {}
        """All registered archives/stores."""
        self._mountpoints: Dict[str, str] = {}
        """All registered mountpoints (key: mountpoint, value: archive_alias)."""

        self._default_alias_store: Union[str, None] = None
        """The alias of the store where new aliases are stored by default."""

        self._dynamic_stores: Union[List[str], None] = None

        self._cached_aliases: Union[Dict[str, AliasItem], None] = None
        self._cached_aliases_by_id: Union[Dict[uuid.UUID, Set[AliasItem]], None] = None

        self._cached_dynamic_aliases: Union[Dict[str, AliasItem], None] = None

    def register_archive(
        self,
        archive: AliasArchive,
        set_as_default_store: Union[bool, None] = None,
        mount_point: Union[str, None] = None,
    ) -> str:

        alias = archive.archive_name

        if not alias:
            raise Exception("Invalid alias archive alias: can't be empty.")

        if not mount_point:
            mount_point = archive.archive_name

        if "#" in mount_point:
            raise Exception(
                f"Can't register alias archive with mountpoint '{alias}': mountpoint is not allowed to contain a '#' character."
            )

        if ":" in mount_point:
            raise Exception(
                f"Can't register alias archive with mountpoint '{alias}': mountpoint is not allowed to contain a ':' character."
            )

        if alias in self._alias_archives.keys():
            raise Exception(f"Can't add store, alias '{alias}' already registered.")

        if mount_point:
            # if mount_point in self.aliases:
            #     raise Exception(
            #         f"Can't mount alias archive: mountpoint '{mount_point}' already in use as alias."
            #     )
            if mount_point in self._mountpoints.keys():
                raise Exception(f"Mountpoint '{mount_point}' already registered.")
            self._mountpoints[mount_point] = alias

        archive.register_archive(kiara=self._kiara)
        self._alias_archives[alias] = archive

        is_store = False
        is_default_store = False
        if archive.is_writeable():
            is_store = True
            if set_as_default_store and self._default_alias_store is not None:
                raise Exception(
                    f"Can't set alias store '{alias}' as default store: default store already set."
                )

            if self._default_alias_store is None:
                is_default_store = True
                self._default_alias_store = alias

        # TODO: add to cache if it already exists instead of invalidating, for performance reasons
        self._cached_aliases = None
        self._cached_aliases_by_id = None
        self._dynamic_stores = None
        self._cached_dynamic_aliases = None

        event = AliasArchiveAddedEvent(
            kiara_id=self._kiara.id,
            alias_archive_id=archive.archive_id,
            alias_archive_alias=alias,
            is_store=is_store,
            is_default_store=is_default_store,
            mount_point=mount_point,
        )
        self._event_callback(event)

        return alias

    @property
    def default_alias_store(self) -> str:

        if self._default_alias_store is None:
            raise Exception("No default alias store set (yet).")
        return self._default_alias_store

    @property
    def alias_archives(self) -> Mapping[str, AliasArchive]:
        return self._alias_archives

    def get_archive(
        self, archive_alias: Union[str, None, uuid.UUID] = None
    ) -> Union[AliasArchive, None]:
        if archive_alias in (None, DEFAULT_STORE_MARKER, DEFAULT_ALIAS_STORE_MARKER):
            archive_alias = self.default_alias_store
            if archive_alias is None:
                raise Exception("Can't retrieve default alias archive, none set (yet).")

        archive = self._alias_archives.get(archive_alias, None)  # type: ignore
        if archive is None:
            if isinstance(archive_alias, str):
                try:
                    archive_alias = uuid.UUID(archive_alias)
                except Exception:
                    pass
            for a in self._alias_archives.values():
                if a.archive_id == archive_alias:
                    return a
        return archive

    @property
    def all_aliases(self) -> Iterable[str]:

        return self.aliases.keys()

    @property
    def aliases_by_id(self) -> Mapping[uuid.UUID, Set[AliasItem]]:
        if self._cached_aliases_by_id is None:
            self.aliases
        return self._cached_aliases_by_id  # type: ignore

    @property
    def dynamic_aliases(self) -> Dict[str, AliasItem]:
        if self._cached_dynamic_aliases is None:
            self.aliases
        return self._cached_dynamic_aliases  # type: ignore

    @property
    def aliases(self) -> Mapping[str, AliasItem]:
        """Retrieve a map of all available aliases, context wide, with the registered archive aliases as values."""
        if self._cached_aliases is not None:
            return self._cached_aliases

        # TODO: multithreading lock
        all_aliases: Dict[str, AliasItem] = {}
        all_aliases_by_id: Dict[uuid.UUID, Set[AliasItem]] = {}
        dynamic_stores = []

        for archive_alias, archive in self._alias_archives.items():

            alias_map = archive.retrieve_all_aliases()
            if alias_map is None:
                dynamic_stores.append(archive_alias)
                continue
            for alias, v_id in alias_map.items():
                if archive_alias == self.default_alias_store:
                    final_alias = alias
                else:
                    final_alias = f"{archive_alias}#{alias}"

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
        self._dynamic_stores = dynamic_stores
        self._cached_dynamic_aliases = {}

        return self._cached_aliases

    @property
    def dynamic_stores(self) -> List[str]:
        if self._dynamic_stores is None:
            self.aliases
        return self._dynamic_stores  # type: ignore

    def find_value_id_for_alias(self, alias: str) -> Union[uuid.UUID, None]:
        """Find the value id for a given alias.

        This method will check all registered archives if they have the alias registered (under their respective mountpoints, if applicable), then it will check the archives that have dynamic aliases (i.e. they don't
        return a list of all available aliases, but 'None' if queried).

        Once found, the value will be stored in a cache for faster retrieval next time.
        """

        alias_item = self.aliases.get(alias, None)
        if alias_item is not None:
            return alias_item.value_id

        alias_item = self.dynamic_aliases.get(alias, None)
        if alias_item is not None:
            return alias_item.value_id

        if "#" not in alias:
            archive_alias: Union[str, None] = self.default_alias_store
            rest = alias
        else:
            mountpoint, rest = alias.split("#", maxsplit=1)
            archive_alias = self._mountpoints.get(mountpoint, None)

            if archive_alias is None:
                return None

        if archive_alias not in self.dynamic_stores:
            return None

        archive = self.get_archive(archive_alias=archive_alias)
        if archive is None:
            raise Exception(f"Invalid alias store: '{archive_alias}' not registered.")
        result_value_id = archive.find_value_id_for_alias(alias=rest)
        if result_value_id:
            alias_item = AliasItem(
                full_alias=alias,
                rel_alias=rest,
                value_id=result_value_id,
                alias_archive=archive_alias,
                alias_archive_id=archive.archive_id,
            )
            self.dynamic_aliases[alias] = alias_item
            return result_value_id
        else:
            return None

    def _get_value_id(self, value_id: Union[uuid.UUID, ValueLink, str]) -> uuid.UUID:
        """Convenience method to ensure a uuid.UUID type for a value id."""

        if not isinstance(value_id, uuid.UUID):
            # fallbacks for common mistakes, this should error out if not a Value or string.
            if hasattr(value_id, "value_id"):
                _value_id: Union[uuid.UUID, str] = value_id.value_id  # type: ignore
                if isinstance(_value_id, str):
                    _value_id = uuid.UUID(_value_id)
            else:
                try:
                    _value_id = uuid.UUID(
                        value_id  # type: ignore
                    )  # this should fail if not string or wrong string format
                except ValueError:
                    raise KiaraException(f"Could not resolve value id for: {value_id}")
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
        """Finds all registered aliases for the provided value id.

        If 'search_dynamic_archives' is set to 'True', this method will also search all dynamic archives for the value id, which is not being done by default for performance reasons.
        """

        value_id = self._get_value_id(value_id=value_id)

        aliases = {a.full_alias for a in self.aliases_by_id.get(value_id, [])}

        if search_dynamic_archives:
            for archive_alias, archive in self._alias_archives.items():
                _aliases = archive.find_aliases_for_value_id(value_id=value_id)
                if _aliases:
                    for a in _aliases:
                        full_alias = f"{archive_alias}#{a}"
                        alias_item = AliasItem(
                            full_alias=full_alias,
                            rel_alias=a,
                            value_id=value_id,
                            alias_archive=archive_alias,
                            alias_archive_id=archive.archive_id,
                        )
                        self.dynamic_aliases[full_alias] = alias_item
                        aliases.add(full_alias)

        return aliases

    def register_aliases(
        self,
        value_id: Union[uuid.UUID, ValueLink, str],
        aliases: Union[str, Iterable[str]],
        allow_overwrite: bool = False,
        alias_store: Union[str, None] = None,
    ):

        value = self._kiara.data_registry.get_value(value=value_id)

        if alias_store in [DEFAULT_STORE_MARKER, DEFAULT_ALIAS_STORE_MARKER, None]:
            alias_store = self.default_alias_store

        if isinstance(aliases, str):
            aliases = [aliases]
        else:
            for alias in aliases:
                if not isinstance(alias, str):
                    raise KiaraException(
                        msg=f"Invalid alias: {alias}.",
                        details="Alias must be a string.",
                    )
                try:
                    uuid.UUID(alias)
                    raise KiaraException(
                        msg=f"Invalid alias name: {alias}.",
                        details="Alias can't be a UUID.",
                    )
                except Exception:
                    pass

        aliases_to_store: Dict[str, List[str]] = {}
        for alias in aliases:
            if "#" in alias:
                mountpoint, alias_name = alias.split("#", maxsplit=1)
                alias_store_alias = self._mountpoints.get(mountpoint, None)
                if alias_store_alias is None:
                    raise Exception(
                        f"Invalid mountpoint: '{mountpoint}' not registered."
                    )

                if alias_store and alias_store != alias_store_alias:
                    raise Exception(
                        f"Can't register alias '{alias}': conflicting alias store references '{alias_store}' != '{alias_store_alias}'."
                    )

                if alias_store:
                    alias_store_alias = alias_store

            else:
                if alias_store:
                    alias_store_alias = alias_store
                else:
                    alias_store_alias = self.default_alias_store
                alias_name = alias

            if alias_name in INVALID_ALIAS_NAMES:
                raise KiaraException(
                    msg=f"Invalid alias name: {alias}.",
                    details=f"The following names can't be used as alias: {', '.join(INVALID_ALIAS_NAMES)}.",
                )

            if "#" in alias_name:
                raise KiaraException(
                    msg=f"Invalid alias name: {alias}.",
                    details="Alias can't contain a '#' character.",
                )
            if ":" in alias_name:
                raise KiaraException(
                    msg=f"Invalid alias name: {alias}.",
                    details="Alias can't contain a ':' character.",
                )

            aliases_to_store.setdefault(alias_store_alias, []).append(alias_name)

        self.aliases  # noqu

        if not allow_overwrite:
            duplicates = []
            for alias in aliases:
                if alias in self.aliases.keys():
                    duplicates.append(alias)

            if duplicates:
                raise Exception(f"Aliases already registered: {duplicates}")

        for store_alias, aliases_for_store in aliases_to_store.items():

            store: AliasStore = self.get_archive(archive_alias=store_alias)  # type: ignore
            if store is None:
                raise Exception(f"Invalid alias store: '{store_alias}' not registered.")
            if not store.is_writeable():
                raise Exception(
                    f"Can't register aliases in store '{store_alias}': store is read-only."
                )

        for store_alias, aliases_for_store in aliases_to_store.items():

            store = self.get_archive(archive_alias=store_alias)  # type: ignore
            store.register_aliases(value.value_id, *aliases_for_store)

            for alias in aliases:
                alias_item = AliasItem(
                    full_alias=alias,
                    rel_alias=alias,
                    value_id=value.value_id,
                    alias_archive=store_alias,
                    alias_archive_id=store.archive_id,
                )

                if store_alias == self.default_alias_store:
                    actual_alias = alias
                else:
                    actual_alias = f"{store_alias}#{alias}"

                if actual_alias in self.aliases.keys():
                    logger.info("alias.replace", alias=actual_alias)
                    # raise NotImplementedError()

                old_value = self.aliases.get(actual_alias)
                if old_value and self._cached_aliases_by_id is not None:
                    to_remove = set()
                    old_aliases = self._cached_aliases_by_id.get(old_value.value_id)
                    if old_aliases:
                        for old_alias in old_aliases:
                            if old_alias.full_alias == actual_alias:
                                to_remove.add(old_alias)

                        self._cached_aliases_by_id[old_value.value_id].remove(
                            *to_remove
                        )

                self.aliases[actual_alias] = alias_item  # type: ignore
                self._cached_aliases_by_id.setdefault(value.value_id, set()).add(alias_item)  # type: ignore


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
