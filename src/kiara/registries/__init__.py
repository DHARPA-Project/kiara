# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import os
import uuid
from typing import TYPE_CHECKING, Any, Generic, Iterable, Mapping, Type, TypeVar, Union

import structlog
from pydantic import BaseModel, ConfigDict, Field

from kiara.utils import log_message

try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self  # type: ignore

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.context.config import KiaraArchiveConfig


class ArchiveConfig(BaseModel, abc.ABC):
    @classmethod
    @abc.abstractmethod
    def create_new_store_config(
        cls, store_id: uuid.UUID, stores_base_path: str
    ) -> Self:
        raise NotImplementedError(
            f"Store config type '{cls}' does not implement 'create_new_config'."
        )

    model_config = ConfigDict()


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
    def create_config(
        cls, config: Union["KiaraArchiveConfig", BaseModel, Mapping[str, Any]]
    ) -> "BaseArchive":

        from kiara.context.config import KiaraArchiveConfig

        if isinstance(config, cls._config_cls):
            config = config
        elif isinstance(config, KiaraArchiveConfig):
            config = cls._config_cls(**config.config)
        elif isinstance(config, BaseModel):
            config = cls._config_cls(**config.model_dump())
        elif isinstance(config, Mapping):
            config = cls._config_cls(**config)

        return config

    def __init__(self, force_read_only: bool = False, **kwargs):
        self._force_read_only: bool = force_read_only

    @classmethod
    @abc.abstractmethod
    def supported_item_types(cls) -> Iterable[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def _is_writeable(cls) -> bool:
        pass

    def is_writeable(self) -> bool:
        if self._force_read_only:
            return False
        return self.__class__._is_writeable()

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

        return hash(self.__class__) + hash(self.archive_id)

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False

        return self.archive_id == other.archive_id


class BaseArchive(KiaraArchive, Generic[ARCHIVE_CONFIG_CLS]):

    _config_cls: Type[ARCHIVE_CONFIG_CLS] = None  # type: ignore

    @classmethod
    def create_new_config(
        cls, store_id: uuid.UUID, stores_base_path: str
    ) -> ARCHIVE_CONFIG_CLS:

        log_message(
            "create_new_store",
            store_id=store_id,
            stores_base_path=stores_base_path,
            store_type=cls.__name__,
        )

        return cls._config_cls.create_new_store_config(
            store_id=store_id, stores_base_path=stores_base_path
        )

    def __init__(
        self,
        archive_id: uuid.UUID,
        config: ARCHIVE_CONFIG_CLS,
        force_read_only: bool = False,
    ):

        super().__init__(force_read_only=force_read_only)
        self._archive_id: uuid.UUID = archive_id
        self._config: ARCHIVE_CONFIG_CLS = config
        self._kiara: Union["Kiara", None] = None

    @classmethod
    def _is_writeable(cls) -> bool:
        return False

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
    @classmethod
    def create_new_store_config(
        cls, store_id: str, stores_base_path: str
    ) -> "FileSystemArchiveConfig":

        archive_path = os.path.abspath(
            os.path.join(stores_base_path, "filesystem_data_store", store_id)
        )

        return FileSystemArchiveConfig(archive_path=archive_path)

    archive_path: str = Field(
        description="The path where the data for this archive is stored."
    )


class SqliteArchiveConfig(ArchiveConfig):
    @classmethod
    def create_new_store_config(
        cls, store_id: str, stores_base_path: str
    ) -> "SqliteArchiveConfig":

        archive_path = os.path.abspath(
            os.path.join(stores_base_path, "sqlite_stores", f"{store_id}.sqlite")
        )

        return SqliteArchiveConfig(sqlite_db_path=archive_path)

    sqlite_db_path: str = Field(
        description="The path where the data for this archive is stored."
    )
