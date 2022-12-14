# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
import os
import structlog
import uuid
from pathlib import Path
from typing import Set, Tuple, Union

from kiara.models.module.destiny import Destiny
from kiara.registries import ArchiveDetails, FileSystemArchiveConfig
from kiara.registries.destinies import DestinyArchive, DestinyStore
from kiara.utils.windows import fix_windows_longpath, fix_windows_symlink

logger = structlog.getLogger()


class FileSystemDestinyArchive(DestinyArchive):

    _archive_type_name = "filesystem_destiny_archive"
    _config_cls = FileSystemArchiveConfig  # type: ignore

    @classmethod
    def is_writeable(cls) -> bool:
        return False

    # @classmethod
    # def create_from_kiara_context(cls, kiara: "Kiara"):
    #
    #     TODO = kiara_app_dirs.user_data_dir
    #     base_path = Path(TODO) / "destiny_store"
    #     base_path.mkdir(parents=True, exist_ok=True)
    #     result = cls(base_path=base_path, store_id=kiara.id)
    #     ID_REGISTRY.update_metadata(
    #         result.get_destiny_archive_id(), kiara_id=kiara.id, obj=result
    #     )
    #     return result

    def __init__(self, archive_id: uuid.UUID, config: FileSystemArchiveConfig):

        super().__init__(archive_id=archive_id, config=config)
        self._base_path: Union[Path, None] = None

        # base_path = config.archive_path
        # if not base_path.is_dir():
        #     raise Exception(
        #         f"Can't create file system archive instance, base path does not exist or is not a folder: {base_path.as_posix()}."
        #     )

        # self._store_id: uuid.UUID = store_id
        # self._base_path: Path = base_path
        # self._destinies_path: Path = self._base_path / "destinies"
        # self._value_id_path: Path = self._base_path / "value_ids"

    @property
    def destiny_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self.config.archive_path).absolute()  # type: ignore
        self._base_path = fix_windows_longpath(self._base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

    def get_archive_details(self) -> ArchiveDetails:

        size = sum(
            f.stat().st_size
            for f in self.destiny_store_path.glob("**/*")
            if f.is_file()
        )
        return ArchiveDetails(size=size)

    @property
    def destinies_path(self) -> Path:
        return self.destiny_store_path / "destinies"

    @property
    def value_id_path(self) -> Path:
        return self.destiny_store_path / "value_ids"

    def _translate_destiny_id_to_path(self, destiny_id: uuid.UUID) -> Path:

        tokens = str(destiny_id).split("-")
        destiny_path = (
            self.destinies_path.joinpath(*tokens[0:-1]) / f"{tokens[-1]}.json"
        )
        return destiny_path

    def _translate_destinies_path_to_id(self, destinies_path: Path) -> uuid.UUID:

        relative = destinies_path.relative_to(self.destinies_path).as_posix()[:-5]

        destninies_id = "-".join(relative.split(os.path.sep))

        return uuid.UUID(destninies_id)

    def _translate_value_id(self, value_id: uuid.UUID, destiny_alias: str) -> Path:

        tokens = str(value_id).split("-")
        value_id_path = self.value_id_path.joinpath(*tokens)

        full_path = value_id_path / f"{destiny_alias}.json"
        return full_path

    def _translate_value_id_path(self, value_path: Path) -> uuid.UUID:

        relative = value_path.relative_to(self.value_id_path)

        value_id_str = "-".join(relative.as_posix().split(os.path.sep))
        return uuid.UUID(value_id_str)

    def _translate_alias_path(self, alias_path: Path) -> Tuple[uuid.UUID, str]:

        value_id = self._translate_value_id_path(alias_path.parent)

        alias = alias_path.name[0:-5]

        return value_id, alias

    def get_all_value_ids(self) -> Set[uuid.UUID]:

        all_root_folders = self.value_id_path.glob("*/*/*/*/*")

        result = set()
        for folder in all_root_folders:
            if not folder.is_dir():
                continue

            value_id = self._translate_value_id_path(folder)
            result.add(value_id)

        return result

    def get_destiny_aliases_for_value(self, value_id: uuid.UUID) -> Set[str]:

        tokens = str(value_id).split("-")
        value_id_path = self.value_id_path.joinpath(*tokens)

        aliases = value_id_path.glob("*.json")

        return set(a.name[0:-5] for a in aliases)

    def get_destiny(self, value_id: uuid.UUID, destiny_alias: str) -> Destiny:

        tokens = str(value_id).split("-")
        value_id_path = self.value_id_path.joinpath(*tokens)

        destiny_path = value_id_path / f"{destiny_alias}.json"

        destiny_data = orjson.loads(destiny_path.read_text())

        destiny = Destiny.construct(**destiny_data)
        return destiny


class FileSystemDestinyStore(FileSystemDestinyArchive, DestinyStore):

    _archive_type_name = "filesystem_destiny_store"

    @classmethod
    def is_writeable(cls) -> bool:
        return True

    def persist_destiny(self, destiny: Destiny):

        destiny_path = self._translate_destiny_id_to_path(destiny_id=destiny.destiny_id)
        destiny_path.parent.mkdir(parents=True, exist_ok=True)
        destiny_path.write_text(destiny.json())

        for value_id in destiny.fixed_inputs.values():

            path = self._translate_value_id(
                value_id=value_id, destiny_alias=destiny.destiny_alias
            )
            if path.exists():
                logger.debug("replace.destiny.file", path=path.as_posix())
                path.unlink()
                # raise Exception(
                #     f"Can't persist destiny '{destiny.destiny_id}': already persisted."
                # )

            path.parent.mkdir(parents=True, exist_ok=True)
            fix_windows_symlink(destiny_path, path)
