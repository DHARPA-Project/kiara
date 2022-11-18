# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Set,
    Tuple,
    Union,
)

from kiara.models.events.destiny_registry import DestinyArchiveAddedEvent
from kiara.models.module.destiny import Destiny
from kiara.models.module.manifest import Manifest
from kiara.models.values.value import Value
from kiara.registries.destinies import DestinyArchive, DestinyStore

if TYPE_CHECKING:
    from kiara.context import Kiara


class DestinyRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

        self._event_callback: Callable = self._kiara.event_registry.add_producer(self)

        self._destiny_archives: Dict[str, DestinyArchive] = {}
        self._default_destiny_store: Union[str, None] = None
        # default_metadata_archive = FileSystemDestinyStore.create_from_kiara_context(
        #     self._kiara
        # )
        # self.register_destiny_archive("metadata", default_metadata_archive)

        self._all_values: Union[Dict[uuid.UUID, Set[str]], None] = None
        self._cached_value_aliases: Dict[
            uuid.UUID, Dict[str, Union[Destiny, None]]
        ] = {}

        self._destinies: Dict[uuid.UUID, Destiny] = {}
        self._destinies_by_value: Dict[uuid.UUID, Dict[str, Destiny]] = {}
        self._destiny_store_map: Dict[uuid.UUID, str] = {}

    @property
    def default_destiny_store(self) -> DestinyStore:

        if self._default_destiny_store is None:
            raise Exception("No default destiny store set (yet).")

        return self._destiny_archives[self._default_destiny_store]  # type: ignore

    @property
    def destiny_archives(self) -> Mapping[str, DestinyArchive]:
        return self._destiny_archives

    def register_destiny_archive(
        self,
        archive: DestinyArchive,
        alias: Union[str, None] = None,
        set_as_default_store: Union[bool, None] = None,
    ):

        destiny_store_id = archive.archive_id
        archive.register_archive(kiara=self._kiara)
        if alias is None:
            alias = str(destiny_store_id)

        if alias in self._destiny_archives.keys():
            raise Exception(
                f"Can't add destiny archive, alias '{alias}' already registered."
            )

        self._destiny_archives[alias] = archive

        is_store = False
        is_default_store = False

        if isinstance(archive, DestinyStore):
            is_store = True

            if set_as_default_store and self._default_destiny_store is not None:
                raise Exception(
                    f"Can't set data store '{alias}' as default store: default store already set."
                )

            if self._default_destiny_store is None or set_as_default_store:
                is_default_store = True
                self._default_destiny_store = alias

        event = DestinyArchiveAddedEvent.construct(
            kiara_id=self._kiara.id,
            destiny_archive_id=archive.archive_id,
            destiny_archive_alias=alias,
            is_store=is_store,
            is_default_store=is_default_store,
        )
        self._event_callback(event)

        # if not registered_name.isalnum():
        #     raise Exception(
        #         f"Can't register destiny archive with name '{registered_name}: name must only contain alphanumeric characters.'"
        #     )
        #
        # if registered_name in self._destiny_archives.keys():
        #     raise Exception(
        #         f"Can't register alias store, store id already registered: {registered_name}."
        #     )
        #
        # self._destiny_archives[registered_name] = alias_store
        #
        # if self._default_destiny_store is None and isinstance(
        #     alias_store, DestinyStore
        # ):
        #     self._default_destiny_store = registered_name

    def _extract_archive(self, alias: str) -> Tuple[str, str]:

        if "." not in alias:
            assert self._default_destiny_store is not None
            return (self._default_destiny_store, alias)

        store_id, rest = alias.split(".", maxsplit=1)

        if store_id not in self._destiny_archives.keys():
            assert self._default_destiny_store is not None
            return (self._default_destiny_store, alias)
        else:
            return (store_id, rest)

    def add_destiny(
        self,
        destiny_alias: str,
        values: Dict[str, uuid.UUID],
        manifest: Manifest,
        result_field_name: Union[str, None] = None,
    ) -> Destiny:
        """Add a destiny for one (or in some rare cases several) values.

        A destiny alias must be unique for every one of the involved input values.
        """

        if not values:
            raise Exception("Can't add destiny, no values provided.")

        store_id, alias = self._extract_archive(destiny_alias)

        destiny = Destiny.create_from_values(
            kiara=self._kiara,
            destiny_alias=alias,
            manifest=manifest,
            result_field_name=result_field_name,
            values=values,
        )

        for value_id in destiny.fixed_inputs.values():
            self._destinies[destiny.destiny_id] = destiny
            # TODO: store history?
            self._destinies_by_value.setdefault(value_id, {})[destiny_alias] = destiny
            self._cached_value_aliases.setdefault(value_id, {})[destiny_alias] = destiny

        self._destiny_store_map[destiny.destiny_id] = store_id

        return destiny

    def get_destiny(self, value_id: uuid.UUID, destiny_alias: str) -> Destiny:

        destiny = self._destinies_by_value.get(value_id, {}).get(destiny_alias, None)
        if destiny is None:
            raise Exception(
                f"No destiny '{destiny_alias}' available for value '{value_id}'."
            )

        return destiny

    @property
    def _all_values_store_map(self) -> Dict[uuid.UUID, Set[str]]:

        if self._all_values is not None:
            return self._all_values

        all_values: Dict[uuid.UUID, Set[str]] = {}
        for archive_id, archive in self._destiny_archives.items():

            all_value_ids = archive.get_all_value_ids()
            for v_id in all_value_ids:
                all_values.setdefault(v_id, set()).add(archive_id)

        self._all_values = all_values
        return self._all_values

    @property
    def all_values(self) -> Iterable[uuid.UUID]:

        all_stored_values = set(self._all_values_store_map.keys())
        all_stored_values.update(self._destinies_by_value.keys())
        return all_stored_values

    def get_destiny_aliases_for_value(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Iterable[str]:

        # TODO: cache the result of this

        if alias_filter is not None:
            raise NotImplementedError()

        all_stores = self._all_values_store_map.get(value_id)
        aliases: Set[str] = set()
        if all_stores:
            for prefix in all_stores:
                all_aliases = self._destiny_archives[
                    prefix
                ].get_destiny_aliases_for_value(value_id=value_id)
                if all_aliases is not None:
                    aliases.update((f"{prefix}.{a}" for a in all_aliases))

        current = self._destinies_by_value.get(value_id, None)
        if current:
            aliases.update(current.keys())

        return sorted(aliases)

    # def get_destinies_for_value(
    #     self,
    #     value_id: uuid.UUID,
    #     destiny_alias_filter: Optional[str] = None
    # ) -> Mapping[str, Destiny]:
    #
    #
    #
    #     return self._destinies_by_value.get(value_id, {})

    def resolve_destiny(self, destiny: Destiny) -> Value:

        results = self._kiara.job_registry.execute_and_retrieve(
            manifest=destiny, inputs=destiny.merged_inputs
        )
        value = results.get_value_obj(field_name=destiny.result_field_name)

        destiny.result_value_id = value.value_id

        return value

    def attach_as_property(
        self,
        destiny: Union[uuid.UUID, Destiny],
        field_names: Union[Iterable[str], None] = None,
    ):

        if field_names:
            raise NotImplementedError()

        if isinstance(destiny, uuid.UUID):
            destiny = self._destinies[destiny]

        values = self._kiara.data_registry.load_values(destiny.fixed_inputs)

        already_stored: List[uuid.UUID] = []
        for v in values.values():
            if v.is_stored:
                already_stored.append(v.value_id)

        if already_stored:
            stored = (str(v) for v in already_stored)
            raise Exception(
                f"Can't attach destiny as property, value(s) already stored: {', '.join(stored)}"
            )

        store_id = self._destiny_store_map[destiny.destiny_id]

        full_path = f"{store_id}.{destiny.destiny_alias}"

        for v in values.values():
            assert destiny.result_value_id is not None
            v.add_property(
                value_id=destiny.result_value_id,
                property_path=full_path,
                add_origin_to_property_value=True,
            )

    def store_destiny(self, destiny_id: Union[Destiny, uuid.UUID]):

        try:
            _destiny_id: uuid.UUID = destiny_id.destiny_id  # type: ignore
        except Exception:
            # just in case this is a 'Destiny' object
            _destiny_id = destiny_id  # type: ignore

        store_id = self._destiny_store_map[_destiny_id]
        destiny = self._destinies[_destiny_id]
        store: DestinyStore = self._destiny_archives[store_id]  # type: ignore

        if not isinstance(store, DestinyStore):
            full_alias = f"{store_id}.{destiny.destiny_alias}"
            raise Exception(
                f"Can't store destiny '{full_alias}': prefix '{store_id}' not writable in this kiara context."
            )

        store.persist_destiny(destiny=destiny)
