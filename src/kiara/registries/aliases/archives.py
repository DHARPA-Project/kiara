# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import shutil
import uuid
from pathlib import Path
from typing import Mapping, Set, Union

from kiara.registries import ARCHIVE_CONFIG_CLS, FileSystemArchiveConfig
from kiara.registries.aliases import AliasArchive, AliasStore
from kiara.utils.windows import fix_windows_longpath


class FileSystemAliasArchive(AliasArchive):

    _archive_type_name = "filesystem_alias_archive"
    _config_cls = FileSystemArchiveConfig  # type: ignore

    def __init__(self, archive_id: uuid.UUID, config: ARCHIVE_CONFIG_CLS):

        super().__init__(archive_id=archive_id, config=config)

        self._base_path: Union[Path, None] = None

    @property
    def alias_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self.config.archive_path).absolute()  # type: ignore
        self._base_path = fix_windows_longpath(self._base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

    @property
    def aliases_path(self) -> Path:
        return self.alias_store_path / "aliases"

    @property
    def value_id_path(self) -> Path:
        return self.alias_store_path / "value_ids"

    def _delete_archive(self):
        shutil.rmtree(self.alias_store_path)

    def _translate_alias(self, alias: str) -> Path:

        if "." in alias:
            tokens = alias.split(".")
            alias_path = (
                self.aliases_path.joinpath(*tokens[0:-1]) / f"{tokens[-1]}.alias"
            )
        else:
            alias_path = self.aliases_path / f"{alias}.alias"
        return alias_path

    def _translate_alias_path(self, alias_path: Path) -> str:

        relative = (
            alias_path.absolute()
            .relative_to(self.aliases_path.absolute())
            .as_posix()[:-6]
        )

        relative = os.path.normpath(relative)

        if os.path.sep not in relative:
            alias = relative
        else:
            alias = ".".join(relative.split(os.path.sep))

        return alias

    def _translate_value_id(self, value_id: uuid.UUID) -> Path:

        tokens = str(value_id).split("-")
        value_id_path = (
            self.value_id_path.joinpath(*tokens[0:-1]) / f"{tokens[-1]}.value"
        )
        return value_id_path

    def _translate_value_path(self, value_path: Path) -> uuid.UUID:

        relative = (
            fix_windows_longpath(value_path.absolute())
            .relative_to(fix_windows_longpath(self.value_id_path.absolute()))
            .as_posix()[:-6]
        )
        relative = os.path.normpath(relative)
        value_id_str = "-".join(relative.split(os.path.sep))

        return uuid.UUID(value_id_str)

    def retrieve_all_aliases(self) -> Mapping[str, uuid.UUID]:

        all_aliases = self.aliases_path.rglob("*.alias")
        result = {}
        for alias_path in all_aliases:
            alias = self._translate_alias_path(alias_path=alias_path)
            value_id = self._find_value_id_for_alias_path(alias_path=alias_path)
            assert value_id is not None
            result[alias] = value_id

        return result

    def find_value_id_for_alias(self, alias: str) -> Union[uuid.UUID, None]:
        alias_path = self._translate_alias(alias)
        if not alias_path.exists():
            return None
        return self._find_value_id_for_alias_path(alias_path=alias_path)

    def _find_value_id_for_alias_path(self, alias_path: Path) -> Union[uuid.UUID, None]:

        resolved = alias_path.resolve()

        assert resolved.name.endswith(".value")

        value_id = self._translate_value_path(value_path=resolved)
        return value_id

    def find_aliases_for_value_id(self, value_id: uuid.UUID) -> Union[Set[str], None]:
        raise NotImplementedError()


class FileSystemAliasStore(FileSystemAliasArchive, AliasStore):

    _archive_type_name = "filesystem_alias_store"

    @classmethod
    def is_writeable(cls) -> bool:
        return True

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
