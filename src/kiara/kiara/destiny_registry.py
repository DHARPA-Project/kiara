import abc
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

from kiara.kiara.id_registry import ID_REGISTRY

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class DestinyArchive(abc.ABC):

    @abc.abstractmethod
    def get_destiny_archive_id(self) -> uuid.UUID:
        pass


class DestinyStore(DestinyArchive):

    pass


class FileSystemDestinyArchive(DestinyArchive):

    @classmethod
    def create_from_kiara_context(cls, kiara: "Kiara"):

        base_path = Path(kiara.context_config.data_directory) / "alias_store"
        base_path.mkdir(parents=True, exist_ok=True)
        result = cls(base_path=base_path, store_id=kiara.id)
        ID_REGISTRY.update_metadata(result.get_destiny_archive_id(), kiara_id=kiara.id, obj=result)
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

    def get_destiny_archive_id(self) -> uuid.UUID:
        return self._store_id

class FileSystemDestinyStore(FileSystemDestinyArchive, DestinyStore):

    pass

class DestinyRegistry(object):

    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._destiny_archives: Dict[uuid.UUID, DestinyArchive] = {}
        self._default_destiny_store: Optional[DestinyStore] = None
        default_archive = FileSystemDestinyStore.create_from_kiara_context(self._kiara)
        self.register_destiny_archive(
            default_archive
        )

    def register_destiny_archive(self, alias_store: DestinyStore):

        as_id = alias_store.get_destiny_archive_id()
        if as_id in self._destiny_archives.keys():
            raise Exception(
                f"Can't register alias store, store id already registered: {as_id}."
            )

        self._destiny_archives[as_id] = alias_store

        if self._default_destiny_store is None and isinstance(alias_store, DestinyStore):
            self._default_destiny_store = alias_store
