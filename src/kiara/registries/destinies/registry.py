# -*- coding: utf-8 -*-
import uuid
from typing import TYPE_CHECKING, Dict, Mapping, Optional, Set, Union

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
        category: str,
        key: str,
        values: Dict[str, Union[Value, uuid.UUID]],
        manifest: Manifest,
        result_field_name: Optional[str] = None,
    ) -> Destiny:

        if not values:
            raise Exception("Can't add destiny, no values provided.")

        value_ids: Dict[str, uuid.UUID] = {
            k: (value if isinstance(value, uuid.UUID) else value.value_id)
            for k, value in values.items()
        }

        duplicates = []
        for value_id in value_ids.values():
            if (
                self._destinies_by_value.get(value_id, {})
                .get(category, {})
                .get(key, None)
                is not None
            ):
                duplicates.append(value_id)

        if duplicates:
            for value_id in duplicates:
                log_message(
                    "duplicate.destiny", category=category, key=key, value_id=value_id
                )
            # raise Exception(f"Can't add destiny, already existing destiny for value(s): {', '.join(duplicates)}.")

        destiny = Destiny.create_from_values(
            kiara=self._kiara,
            category=category,
            key=key,
            manifest=manifest,
            result_field_name=result_field_name,
            values=value_ids,
        )

        for value_id in value_ids.values():
            self._destinies[destiny.destiny_id] = destiny
            self._destinies_by_value.setdefault(value_id, {}).setdefault(
                category, {}
            ).setdefault(key, set()).add(destiny)

        return destiny

    def get_destinies_for_value(
        self,
        value: Union[uuid.UUID, Value],
        category: Optional[str] = None,
        key: Optional[str] = None,
    ) -> Mapping[str, Mapping[str, Set[Destiny]]]:

        if isinstance(value, Value):
            value = value.value_id

        destinies = self._destinies_by_value.get(value, {})
        if category is not None:
            temp = {}
            temp[category] = destinies.get(category, {})
            destinies = temp

        if key is not None:
            temp = {}
            for category, keys in destinies.items():
                for _key, _destinies in keys.items():
                    if key == _key:
                        temp.setdefault(category, {})[key] = _destinies

            destinies = temp
        return destinies

    def resolve_destinies_for_value(
        self,
        value: Union[uuid.UUID, Value],
        category: Optional[str] = None,
        key: Optional[str] = None,
    ) -> Dict[str, Dict[str, Dict[uuid.UUID, Value]]]:

        result: Dict[str, Dict[str, Dict[uuid.UUID, Value]]] = {}
        destinies = self.get_destinies_for_value(
            value=value, category=category, key=key
        )
        for category, keys in destinies.items():
            for key, destinies in keys.items():
                for destiny in destinies:
                    value = self.resolve_destiny(destiny)
                    result.setdefault(category, {}).setdefault(key, {})[
                        destiny.destiny_id
                    ] = value

        return result

    def resolve_destiny(self, destiny: Destiny) -> Value:

        results = self._kiara.job_registry.execute_and_retrieve(
            manifest=destiny, inputs=destiny.merged_inputs
        )
        value = results.get_value_obj(field_name=destiny.result_field_name)
        destiny.result_value_id = value.value_id
        return value
