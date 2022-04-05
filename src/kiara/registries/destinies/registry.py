# -*- coding: utf-8 -*-
import uuid
from typing import TYPE_CHECKING, Dict, Mapping, Optional, Set, Union, Iterable

from kiara.models.module.destiniy import Destiny
from kiara.models.module.manifest import Manifest
from kiara.models.values.value import Value
from kiara.registries.destinies import DestinyArchive, DestinyStore
from kiara.registries.destinies.file_system_store import FileSystemDestinyStore
from kiara.utils import log_message

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class DestinyRegistry(object):


    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._destiny_archives: Dict[uuid.UUID, DestinyArchive] = {}
        self._default_destiny_store: Optional[DestinyStore] = None
        default_archive = FileSystemDestinyStore.create_from_kiara_context(self._kiara)
        self.register_destiny_archive(default_archive)

        self._destinies: Dict[uuid.UUID, Destiny] = {}
        self._destiny_value_map: Dict[uuid.UUID, Dict[str, Set[uuid.UUID]]] = {}
        self._destinies_by_value: Dict[uuid.UUID, Dict[str, Destiny]] = {}

    @property
    def default_destiny_store(self) -> DestinyStore:

        if self._default_destiny_store is None:
            raise Exception("No default destiny store set (yet).")

        return self._default_destiny_store

    def register_destiny_archive(self, alias_store: DestinyStore):

        as_id = alias_store.get_destiny_archive_id()
        if as_id in self._destiny_archives.keys():
            raise Exception(
                f"Can't register alias store, store id already registered: {as_id}."
            )

        self._destiny_archives[as_id] = alias_store

        if self._default_destiny_store is None and isinstance(
            alias_store, DestinyStore
        ):
            self._default_destiny_store = alias_store

    def add_destiny(
        self,
        destiny_alias: str,
        values: Dict[str, uuid.UUID],
        manifest: Manifest,
        result_field_name: Optional[str] = None,
    ) -> Destiny:

        if not values:
            raise Exception("Can't add destiny, no values provided.")

        destiny = Destiny.create_from_values(
            kiara=self._kiara,
            destiny_alias=destiny_alias,
            manifest=manifest,
            result_field_name=result_field_name,
            values=values,
        )

        for value_id in destiny.fixed_inputs.values():
            self._destinies[destiny.destiny_id] = destiny
            # TODO: store history?
            self._destinies_by_value.setdefault(value_id, {})[destiny_alias] = destiny
            self._destiny_value_map.setdefault(destiny.destiny_id, {}).setdefault(destiny_alias, set()).add(value_id)

        return destiny

    def get_destinies_for_value(
        self,
        value_id: uuid.UUID,
        destiny_alias_filter: Optional[str] = None
    ) -> Mapping[str, Destiny]:


        if destiny_alias_filter is not None:
            raise NotImplementedError()

        return self._destinies_by_value.get(value_id, {})


    def resolve_destiny(self, destiny: Destiny) -> Value:

        results = self._kiara.job_registry.execute_and_retrieve(
            manifest=destiny, inputs=destiny.merged_inputs
        )
        value = results.get_value_obj(field_name=destiny.result_field_name)

        return value

    def store_destiny(self, destiny_id: uuid.UUID):

        destiny_map = self._destiny_value_map.get(destiny_id, None)
        if destiny_id is None:
            raise Exception(f"Can't persist destiny '{destiny_id}': no such destiny registered.")

        destiny = self._destinies[destiny_id]

        for alias, values in destiny_map.items():
            self._default_destiny_store.persist_destiny(value_ids=values, destiny_alias=alias, destiny=destiny)
