# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import os
import uuid
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterable,
    Mapping,
    Type,
    TypeVar,
    Union,
)

import structlog
from pydantic import BaseModel, ConfigDict, Field, RootModel, field_validator

from kiara.defaults import (
    ARCHIVE_NAME_MARKER,
    CHUNK_COMPRESSION_TYPE,
    DEFAULT_CHUNK_COMPRESSION,
)
from kiara.utils import log_message

try:
    from typing import Literal  # type: ignore
except ImportError:
    from typing_extensions import Literal  # type: ignore
try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self  # type: ignore

if TYPE_CHECKING:
    from kiara.context import Kiara


class ArchiveConfig(BaseModel, abc.ABC):
    @classmethod
    @abc.abstractmethod
    def create_new_store_config(cls, store_base_path: str, **kwargs) -> Self:
        raise NotImplementedError(
            f"Store config type '{cls}' does not implement 'create_new_config'."
        )

    model_config = ConfigDict()

    # @abc.abstractmethod
    # def get_archive_id(self) -> uuid.UUID:
    #     raise NotImplementedError(
    #         f"Store config type '{self.__class__.__name__}' does not implement 'get_archive_id'."
    #     )


ARCHIVE_CONFIG_CLS = TypeVar("ARCHIVE_CONFIG_CLS", bound=ArchiveConfig)


logger = structlog.getLogger()


class ArchiveDetails(RootModel):
    root: Dict[str, Any]


class ArchiveMetadata(RootModel):
    root: Mapping[str, Any]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

    def __setitem__(self, key, value):
        self.root[key] = value

    def get(self, key, default=None):
        return self.root.get(key, default)

    # archive_id: Union[uuid.UUID, None] = Field(
    #     description="The id of the stored archive.", default=None
    # )
    # custom_metadata: Dict[str, Any] = Field(
    #     description="Custom metadata for the archive.", default_factory=dict
    # )


NON_ARCHIVE_DETAILS = ArchiveDetails(root={})


class KiaraArchive(abc.ABC, Generic[ARCHIVE_CONFIG_CLS]):

    _config_cls: Type[ARCHIVE_CONFIG_CLS] = None  # type: ignore

    # @classmethod
    # def create_store_config_instance(
    #     cls, config: Union[ARCHIVE_CONFIG_CLS, BaseModel, Mapping[str, Any]]
    # ) -> "BaseArchive":
    #     """Create a store config instance from a config instance of a few different types."""
    #
    #     from kiara.context.config import KiaraArchiveConfig
    #
    #     if isinstance(config, cls._config_cls):
    #         config = config
    #     elif isinstance(config, KiaraArchiveConfig):
    #         config = cls._config_cls(**config.config)
    #     elif isinstance(config, BaseModel):
    #         config = cls._config_cls(**config.model_dump())
    #     elif isinstance(config, Mapping):
    #         config = cls._config_cls(**config)
    #
    #     return config

    # @classmethod
    # def is_valid_archive(cls, store_uri: str, **kwargs: Any) -> bool:
    #     return False

    @classmethod
    def _load_archive_config(
        cls, archive_uri: str, allow_write_access: bool, **kwargs
    ) -> Union[Dict[str, Any], None]:
        """Tries to assemble an archive config from an uri (and optional paramters).

        If the archive type supports the archive at the uri, then a valid config will be returned,
        otherwise 'None'.
        """

        return None

    @classmethod
    def load_archive_config(
        cls, archive_uri: str, allow_write_access: bool, **kwargs
    ) -> Union[Dict[str, Any], None]:

        log_message(
            "attempt_loading_existing_store",
            archive_uri=archive_uri,
            archive_type=cls.__name__,
        )

        return cls._load_archive_config(
            archive_uri=archive_uri, allow_write_access=allow_write_access, **kwargs
        )

    @classmethod
    def create_new_store_config(
        cls, store_base_path: str, **kwargs
    ) -> ARCHIVE_CONFIG_CLS:

        log_message(
            "create_new_store",
            store_base_path=store_base_path,
            store_type=cls.__name__,
        )

        Path(store_base_path).mkdir(parents=True, exist_ok=True)

        archive_config: ARCHIVE_CONFIG_CLS = cls._config_cls.create_new_store_config(
            store_base_path=store_base_path, **kwargs
        )
        return archive_config

    def __init__(
        self,
        archive_config: ARCHIVE_CONFIG_CLS,
        force_read_only: bool = False,
        archive_name: Union[str, None] = None,
    ):

        self._archive_instance_name: Union[str, None] = archive_name
        self._config: ARCHIVE_CONFIG_CLS = archive_config
        self._force_read_only: bool = force_read_only

        self._archive_metadata: Union[ArchiveMetadata, None] = None

    @property
    def archive_metadata(self) -> ArchiveMetadata:

        if self._archive_metadata is None:
            archive_metadata = self._retrieve_archive_metadata()
            self._archive_metadata = ArchiveMetadata(root=archive_metadata)

        return self._archive_metadata

    @classmethod
    @abc.abstractmethod
    def supported_item_types(cls) -> Iterable[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def _is_writeable(cls) -> bool:
        pass

    @abc.abstractmethod
    def register_archive(self, kiara: "Kiara"):
        pass

    @abc.abstractmethod
    def _retrieve_archive_metadata(self) -> Mapping[str, Any]:
        """Retrieve metadata for the archive.

        Must contain at least one key 'archive_id', with a uuid-able value that
        uniquely identifies the archive.
        """

        raise NotImplementedError()

    def get_archive_metadata(self, key: str) -> Any:

        return self.archive_metadata.get(key, None)

    def set_archive_metadata_value(self, key: str, value: Any):

        if not self.is_writeable():
            raise Exception("Can't set metadata on read-only archive.")

        self._set_archive_metadata_value(key, value)
        self.archive_metadata[key] = value

    def _set_archive_metadata_value(self, key: str, value: Any):
        """Set custom metadata for the archive."""

        raise NotImplementedError(
            f"This archive type '{type(self.__class__)}' does not support setting metadata."
        )

    @property
    def archive_name(self) -> str:
        if self._archive_instance_name:
            return self._archive_instance_name

        alias = self.get_archive_metadata(ARCHIVE_NAME_MARKER)
        if not alias:
            alias = str(self.archive_id)
        self._archive_instance_name = alias
        return self._archive_instance_name  # type: ignore

    def is_force_read_only(self) -> bool:
        return self._force_read_only

    def set_force_read_only(self, force_read_only: bool):
        self._force_read_only = force_read_only

    def is_writeable(self) -> bool:
        if self._force_read_only:
            return False
        return self.__class__._is_writeable()

    # @abc.abstractmethod
    # def register_archive(self, kiara: "Kiara"):
    #     pass

    @property
    def archive_id(self) -> uuid.UUID:

        try:
            result = self.archive_metadata["archive_id"]
        except KeyError:
            raise Exception("Archive does not have an id metadata value set.")
        return uuid.UUID(result)

    @property
    def config(self) -> ARCHIVE_CONFIG_CLS:
        return self._config

    def get_archive_details(self) -> ArchiveDetails:
        return NON_ARCHIVE_DETAILS

    def delete_archive(self, archive_id: Union[uuid.UUID, None] = None):

        if archive_id != self.archive_id:
            raise Exception(
                f"Not deleting archive with id '{self.archive_id}': confirmation id '{archive_id}' does not match."
            )

        logger.info(
            "deleting.archive",
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


class BaseArchive(KiaraArchive[ARCHIVE_CONFIG_CLS], Generic[ARCHIVE_CONFIG_CLS]):
    """A base class that can be used to implement a kiara archive."""

    def __init__(
        self,
        archive_name: str,
        archive_config: ARCHIVE_CONFIG_CLS,
        force_read_only: bool = False,
    ):

        super().__init__(
            archive_name=archive_name,
            archive_config=archive_config,
            force_read_only=force_read_only,
        )
        self._kiara: Union["Kiara", None] = None

    @classmethod
    def _is_writeable(cls) -> bool:
        return False

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
    def load_store_config(cls, store_uri: str, **kwargs) -> Self:
        raise NotImplementedError(
            f"Store config type '{cls}' does not implement 'create_config'."
        )

    @classmethod
    def create_new_store_config(
        cls, store_base_path: str, **kwargs
    ) -> "FileSystemArchiveConfig":

        store_id = str(uuid.uuid4())
        if "path" in kwargs:
            file_name = kwargs["path"]
        else:
            file_name = store_id

        archive_path = os.path.abspath(os.path.join(store_base_path, file_name))

        return FileSystemArchiveConfig(archive_path=archive_path)

    archive_path: str = Field(
        description="The path where the data for this archive is stored."
    )


class SqliteArchiveConfig(ArchiveConfig):
    @classmethod
    def create_new_store_config(
        cls, store_base_path: str, **kwargs
    ) -> "SqliteArchiveConfig":

        store_id = str(uuid.uuid4())

        if "file_name" in kwargs:
            file_name = kwargs["file_name"]
        else:
            file_name = f"{store_id}.sqlite"

        archive_path = os.path.abspath(os.path.join(store_base_path, file_name))

        if not os.path.exists(archive_path):
            Path(archive_path).parent.mkdir(exist_ok=True, parents=True)

        import sqlite3

        conn = sqlite3.connect(archive_path)

        # Create a cursor object
        c = conn.cursor()

        # Create table
        c.execute(
            """CREATE TABLE IF NOT EXISTS archive_metadata
                     (key text PRIMARY KEY , value text NOT NULL)"""
        )

        # Insert a row of data
        c.execute(
            "INSERT OR IGNORE INTO archive_metadata VALUES ('archive_id', ?)",
            (store_id,),
        )

        # Save (commit) the changes
        conn.commit()

        # Close the connection
        conn.close()

        use_wal_mode = kwargs.get("wal_mode", False)

        return SqliteArchiveConfig(
            sqlite_db_path=archive_path, use_wal_mode=use_wal_mode
        )

    sqlite_db_path: str = Field(
        description="The path where the data for this archive is stored."
    )
    use_wal_mode: bool = Field(
        description="Whether to use WAL mode for the SQLite database.", default=False
    )


class SqliteDataStoreConfig(SqliteArchiveConfig):
    @classmethod
    def create_new_store_config(
        cls, store_base_path: str, **kwargs
    ) -> "SqliteDataStoreConfig":

        store_id = str(uuid.uuid4())

        if "file_name" in kwargs:
            file_name = kwargs["file_name"]
        else:
            file_name = f"{store_id}.sqlite"

        default_chunk_compression = kwargs.get(
            "default_chunk_compression", DEFAULT_CHUNK_COMPRESSION
        )

        archive_path = os.path.abspath(os.path.join(store_base_path, file_name))

        if os.path.exists(archive_path):
            raise Exception(f"Archive path '{archive_path}' already exists.")

        Path(archive_path).parent.mkdir(exist_ok=True, parents=True)

        # Connect to the SQLite database (or create it if it doesn't exist)
        import sqlite3

        conn = sqlite3.connect(archive_path)

        # Create a cursor object
        c = conn.cursor()

        # Create table
        c.execute(
            """CREATE TABLE archive_metadata
                     (key text PRIMARY KEY , value text NOT NULL)"""
        )

        # Insert a row of data
        c.execute("INSERT INTO archive_metadata VALUES ('archive_id', ?)", (store_id,))

        # Save (commit) the changes
        conn.commit()

        # Close the connection
        conn.close()

        use_wal_mode = kwargs.get("wal_mode", False)

        return SqliteDataStoreConfig(
            sqlite_db_path=archive_path,
            default_chunk_compression=default_chunk_compression,
            use_wal_mode=use_wal_mode,
        )

    default_chunk_compression: Literal["none", "lz4", "zstd", "lzma"] = Field(  # type: ignore
        description="The default compression type to use for data in this store.",
        default=DEFAULT_CHUNK_COMPRESSION.ZSTD.name.lower(),  # type: ignore
    )

    @field_validator("default_chunk_compression", mode="before")
    def validate_compression(cls, v):

        if v is None:
            v = "none"
        elif isinstance(v, CHUNK_COMPRESSION_TYPE):
            v = v.name

        return v.lower()
