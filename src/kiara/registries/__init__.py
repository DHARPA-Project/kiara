# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import orjson
import structlog
import uuid
from pydantic import BaseModel, Field
from typing import TYPE_CHECKING, Generic, Iterable, Type, TypeVar, Union

from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from kiara.context import Kiara


class ArchiveConfig(BaseModel):
    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


ARCHIVE_CONFIG_CLS = TypeVar("ARCHIVE_CONFIG_CLS", bound=ArchiveConfig)


logger = structlog.getLogger()


class ArchiveDetails(BaseModel):

    size: Union[int, None] = Field(
        description="The size of the stored archive.", default=None
    )


NON_ARCHIVE_DETAILS = ArchiveDetails()


class KiaraArchive(abc.ABC):

    _config_cls = ArchiveConfig  # type: ignore

    @classmethod
    @abc.abstractmethod
    def supported_item_types(cls) -> Iterable[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def is_writeable(cls) -> bool:
        pass

    @abc.abstractmethod
    def register_archive(self, kiara: "Kiara"):
        pass

    @abc.abstractmethod
    def retrieve_archive_id(self) -> uuid.UUID:
        pass

    @property
    def archive_id(self) -> uuid.UUID:
        return self.retrieve_archive_id()

    @property
    def config(self) -> ArchiveConfig:
        return self._get_config()

    @abc.abstractmethod
    def _get_config(self) -> ArchiveConfig:
        pass

    def get_archive_details(self) -> ArchiveDetails:
        return NON_ARCHIVE_DETAILS

    def delete_archive(self, archive_id: Union[uuid.UUID, None] = None):

        if archive_id != self.archive_id:
            raise Exception(
                f"Not deleting archive with id '{self.archive_id}': confirmation id '{archive_id}' does not match."
            )

        logger.info(
            "deleteing.archive",
            archive_id=self.archive_id,
            item_types=self.supported_item_types(),
            archive_type=self.__class__.__name__,
        )
        self._delete_archive()

    @abc.abstractmethod
    def _delete_archive(self):
        pass

    def __hash__(self):
        return hash(self.archive_id)

    def __eq__(self, other):

        if not isinstance(other, self.__class__):
            return False

        return self.archive_id == other.archive_id


class BaseArchive(KiaraArchive, Generic[ARCHIVE_CONFIG_CLS]):

    _config_cls: Type[ARCHIVE_CONFIG_CLS] = ArchiveConfig  # type: ignore

    def __init__(self, archive_id: uuid.UUID, config: ARCHIVE_CONFIG_CLS):

        self._archive_id: uuid.UUID = archive_id
        self._config: ARCHIVE_CONFIG_CLS = config
        self._kiara: Union["Kiara", None] = None

    def _get_config(self) -> ARCHIVE_CONFIG_CLS:
        return self._config

    def retrieve_archive_id(self) -> uuid.UUID:
        return self._archive_id

    @property
    def kiara_context(self) -> "Kiara":
        if self._kiara is None:
            raise Exception("Archive not registered into a kiara context yet.")
        return self._kiara

    def register_archive(self, kiara: "Kiara"):
        if self._kiara is not None:
            raise Exception("Archive already registered in a context.")
        self._kiara = kiara

    def _delete_archive(self):

        logger.info(
            "ignore.archive_delete_request",
            reason="not implemented/applicable",
            archive_id=self.archive_id,
            item_types=self.supported_item_types(),
            archive_type=self.__class__.__name__,
        )


class FileSystemArchiveConfig(ArchiveConfig):

    archive_path: str = Field(
        description="The path where the data for this archive is stored."
    )
