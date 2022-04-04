# -*- coding: utf-8 -*-
import uuid
from pathlib import Path
from typing import Mapping, Optional, Set

from kiara.models.module.destiniy import Destiny
from kiara.models.values.value import Value
from kiara.models.values.value_schema import ValueSchema
from kiara.registries.destinies import DestinyArchive, DestinyStore
from kiara.registries.ids import ID_REGISTRY


class FileSystemDestinyArchive(DestinyArchive):
    @classmethod
    def create_from_kiara_context(cls, kiara: "Kiara"):

        base_path = Path(kiara.context_config.data_directory) / "alias_store"
        base_path.mkdir(parents=True, exist_ok=True)
        result = cls(base_path=base_path, store_id=kiara.id)
        ID_REGISTRY.update_metadata(
            result.get_destiny_archive_id(), kiara_id=kiara.id, obj=result
        )
        return result

    def __init__(self, base_path: Path, store_id: uuid.UUID):

        if not base_path.is_dir():
            raise Exception(
                f"Can't create file system archive instance, base path does not exist or is not a folder: {base_path.as_posix()}."
            )

        self._store_id: uuid.UUID = store_id
        self._base_path: Path = base_path

    def get_destiny_archive_id(self) -> uuid.UUID:
        return self._store_id

    def get_destinies_for(
        self, value_id: uuid.UUID
    ) -> Optional[Mapping[str, ValueSchema]]:
        pass

    def get_destiny(self, value_id: uuid.UUID, destiny: str) -> Destiny:
        pass


class FileSystemDestinyStore(FileSystemDestinyArchive, DestinyStore):
    def _persist_destinies(
        self, value: Value, category: str, key: str, destinies: Set[Destiny]
    ):

        base_path = self.get_path(EntityType.DESTINY)

        for destiny in destinies:
            path = (
                base_path
                / str(value.value_id)
                / category
                / key
                / f"destiny__{destiny.destiny_id}.json"
            )
            if path.exists():
                raise Exception(
                    f"Can't persist destiny '{destiny.destiny_id}': already persisted."
                )

            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(orjson_dumps(destiny.dict()))
