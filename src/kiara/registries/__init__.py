# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import orjson
import uuid
from pydantic import BaseModel, Field
from typing import TYPE_CHECKING, ClassVar, Generic, Iterable, Optional, Type, TypeVar

from kiara.utils import orjson_dumps

if TYPE_CHECKING:
    from kiara.context import Kiara


class ArchiveConfig(BaseModel):
    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


ARCHIVE_CONFIG_CLS = TypeVar("ARCHIVE_CONFIG_CLS", bound=ArchiveConfig)


class KiaraArchive(abc.ABC):

    _config_cls: ClassVar[Type[ArchiveConfig]] = ArchiveConfig

    @classmethod
    @abc.abstractmethod
    def supported_item_types(cls) -> Iterable[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def is_writeable(cls) -> bool:
        pass

    @abc.abstractmethod
    def register_archive(self, kiara: "Kiara") -> uuid.UUID:
        pass


class BaseArchive(KiaraArchive, Generic[ARCHIVE_CONFIG_CLS]):

    _config_cls = ArchiveConfig

    def __init__(self, archive_id: uuid.UUID, config: ARCHIVE_CONFIG_CLS):

        self._archive_id: uuid.UUID = archive_id
        self._config: ARCHIVE_CONFIG_CLS = config
        self._kiara: Optional["Kiara"] = None

    @property
    def config(self) -> ARCHIVE_CONFIG_CLS:

        return self._config

    @property
    def archive_id(self) -> uuid.UUID:
        return self._archive_id

    @property
    def kiara_context(self) -> "Kiara":
        if self._kiara is None:
            raise Exception("Archive not registered into a kiara context yet.")
        return self._kiara

    def register_archive(self, kiara: "Kiara") -> uuid.UUID:
        self._kiara = kiara
        return self.archive_id


class FileSystemArchiveConfig(ArchiveConfig):

    archive_path: str = Field(
        description="The path where the data for this archive is stored."
    )
