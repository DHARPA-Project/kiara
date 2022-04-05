# -*- coding: utf-8 -*-
import os
import uuid
from pathlib import Path
from typing import Mapping, Optional, Set, Iterable, Tuple

import orjson

from kiara.models.module.destiniy import Destiny
from kiara.models.values.value import Value
from kiara.models.values.value_schema import ValueSchema
from kiara.registries.destinies import DestinyArchive, DestinyStore
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import orjson_dumps


class FileSystemDestinyArchive(DestinyArchive):

    @classmethod
    def create_from_kiara_context(cls, kiara: "Kiara"):

        base_path = Path(kiara.context_config.data_directory) / "destiny_store"
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
        self._destinies_path: Path = self._base_path / "destinies"
        self._value_id_path: Path = self._base_path / "value_ids"

    @property
    def destiny_store_path(self) -> Path:
        return self._base_path

    def get_destiny_archive_id(self) -> uuid.UUID:
        return self._store_id

    def _translate_destiny_id_to_path(self, destiny_id: uuid.UUID) -> Path:

        tokens = str(destiny_id).split("-")
        destiny_path = (
            self._destinies_path.joinpath(*tokens[0:-1]) / f"{tokens[-1]}.json"
        )
        return destiny_path

    def _translate_destinies_path_to_id(self, destinies_path: Path) -> uuid.UUID:

        relative = destinies_path.relative_to(self._destinies_path).as_posix()[:-5]

        destninies_id = "-".join(relative.split(os.path.sep))

        return uuid.UUID(destninies_id)

    def _translate_value_id(self, value_id: uuid.UUID, destiny_alias) -> Path:

        tokens = str(value_id).split("-")
        value_id_path = self._value_id_path.joinpath(*tokens)

        full_path = value_id_path / f'{destiny_alias}.json'
        return full_path

    def _translate_value_path(self, value_path: Path) -> Tuple[uuid.UUID, str]:

        relative = value_path.relative_to(self._value_id_path)

        alias = relative.name[0:-5]
        value_id_str = "-".join(relative.parent.name.split(os.path.sep))

        return uuid.UUID(value_id_str), alias


    def get_destinies_for(
        self, value_id: uuid.UUID
    ) -> Optional[Mapping[str, ValueSchema]]:

        raise NotImplementedError()

    def get_destiny(self, value_id: uuid.UUID, destiny_alias: str) -> Destiny:

        tokens = str(value_id).split("-")
        value_id_path = self._value_id_path.joinpath(*tokens)

        destiny_path = value_id_path / f"{destiny_alias}.json"

        destiny_data = orjson.loads(destiny_path)

        destiny = Destiny.construct(**destiny_data)
        return destiny


class FileSystemDestinyStore(FileSystemDestinyArchive, DestinyStore):

    def persist_destiny(
        self, value_ids: Iterable[Value], destiny_alias: str, destiny: Destiny
    ):

        print("PERSISTING")

        destiny_path = self._translate_destiny_id_to_path(destiny_id=destiny.destiny_id)
        destiny_path.parent.mkdir(parents=True, exist_ok=True)
        destiny_path.write_text(destiny.json())

        for value_id in value_ids:

            path = self._translate_value_id(value_id=value_id, destiny_alias=destiny_alias)
            if path.exists():
                # TODO: maybe version, or check if same?
                path.unlink()
                # raise Exception(
                #     f"Can't persist destiny '{destiny.destiny_id}': already persisted."
                # )

            path.parent.mkdir(parents=True, exist_ok=True)
            path.symlink_to(destiny_path)



