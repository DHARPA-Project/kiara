# -*- coding: utf-8 -*-
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Mapping, Union

from pydantic import Field, PrivateAttr

from kiara.defaults import CHUNK_COMPRESSION_TYPE, DEFAULT_CHUNK_COMPRESSION
from kiara.models import KiaraModel

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.registries.aliases import AliasArchive, AliasStore
    from kiara.registries.data import DataArchive, DataStore
    from kiara.registries.jobs import JobArchive, JobStore
    from kiara.registries.metadata import MetadataArchive, MetadataStore


class KiArchive(KiaraModel):
    @classmethod
    def load_kiarchive(
        cls,
        kiara: "Kiara",
        path: Union[str, Path],
        allow_write_access: bool = False,
        archive_name: Union[str, None] = None,
    ) -> "KiArchive":

        if isinstance(path, Path):
            path = path.as_posix()

        archive_path = Path(path).absolute()

        if not archive_path.is_file():
            raise FileNotFoundError(f"Archive file '{path}' does not exist.")

        from kiara.utils.stores import check_external_archive

        archives = check_external_archive(
            archive=archive_path.as_posix(),
            allow_write_access=allow_write_access,
            archive_name=archive_name,
        )

        if "metadata" in archives.keys():
            metadata_archive: Union[MetadataArchive, None] = archives["metadata"]  # type: ignore
            metadata_archive_config: Union[Mapping[str, Any], None] = metadata_archive.config.model_dump()  # type: ignore
        else:
            metadata_archive_config = None
            metadata_archive = None

        if "data" in archives.keys():
            data_archive: Union[DataArchive, None] = archives["data"]  # type: ignore
            data_archive_config: Union[Mapping[str, Any], None] = data_archive.config.model_dump()  # type: ignore
        else:
            data_archive_config = None
            data_archive = None

        if "alias" in archives.keys():
            alias_archive: Union[AliasArchive, None] = archives["alias"]  # type: ignore
            alias_archive_config: Union[Mapping[str, Any], None] = alias_archive.config.model_dump()  # type: ignore
        else:
            alias_archive_config = None
            alias_archive = None

        if "job_record" in archives.keys():
            jobs_archive: Union[JobArchive, None] = archives["job_record"]  # type: ignore
            jobs_archive_config: Union[Mapping[str, Any], None] = jobs_archive.config.model_dump()  # type: ignore
        else:
            jobs_archive_config = None
            jobs_archive = None

        _archives = [
            x
            for x in (data_archive, alias_archive, metadata_archive, jobs_archive)
            if x is not None
        ]
        if not _archives:
            raise Exception(f"No archive found in file: {path}")
        else:
            archive_id = _archives[0].archive_id
            archive_alias = _archives[0].archive_name
            for archive in _archives:
                if archive.archive_id != archive_id:
                    raise Exception(
                        f"Multiple different archive ids found in file: {path}"
                    )
                if archive.archive_name != archive_alias:
                    raise Exception(
                        f"Multiple different archive aliases found in file: {path}"
                    )

        kiarchive = KiArchive(
            archive_id=archive_id,
            archive_name=archive_alias,
            metadata_archive_config=metadata_archive_config,
            data_archive_config=data_archive_config,
            alias_archive_config=alias_archive_config,
            job_archive_config=jobs_archive_config,
            archive_base_path=archive_path.parent.as_posix(),
            archive_file_name=archive_path.name,
            allow_write_access=allow_write_access,
        )

        kiarchive._metadata_archive = metadata_archive
        kiarchive._data_archive = data_archive
        kiarchive._alias_archive = alias_archive
        kiarchive._jobs_archive = jobs_archive
        kiarchive._kiara = kiara

        return kiarchive

    @classmethod
    def create_kiarchive(
        cls,
        kiara: "Kiara",
        kiarchive_uri: Union[str, Path],
        archive_name: Union[str, None] = None,
        compression: Union[None, CHUNK_COMPRESSION_TYPE, str] = None,
        allow_write_access: bool = True,
        allow_existing: bool = False,
    ) -> "KiArchive":

        if compression is None:
            compression = DEFAULT_CHUNK_COMPRESSION

        if isinstance(kiarchive_uri, str):
            kiarchive_uri = Path(kiarchive_uri)

        if not archive_name:
            archive_name = kiarchive_uri.name
            if archive_name.endswith(".kiarchive"):
                archive_name = archive_name[:-10]

        if kiarchive_uri.exists():
            if not allow_existing:
                raise FileExistsError(f"Archive file '{kiarchive_uri}' already exists.")
            kiarchive = cls.load_kiarchive(
                kiara=kiara, path=kiarchive_uri, allow_write_access=allow_write_access
            )
        else:
            from kiara.utils.stores import create_new_archive

            archive_base_path = kiarchive_uri.parent.as_posix()
            archive_file_name = kiarchive_uri.name

            if isinstance(compression, str):
                compression = CHUNK_COMPRESSION_TYPE[compression.upper()]

            data_store: DataStore = create_new_archive(  # type: ignore
                archive_name=archive_name,
                store_base_path=archive_base_path,
                store_type="sqlite_data_store",
                file_name=archive_file_name,
                default_chunk_compression=str(compression.name),
                allow_write_access=allow_write_access,
            )
            data_store_config = data_store.config

            metadata_store: MetadataStore = create_new_archive(  # type: ignore
                archive_name=archive_name,
                store_base_path=archive_base_path,
                store_type="sqlite_metadata_store",
                file_name=archive_file_name,
                allow_write_access=True,
                set_archive_name_metadata=False,
            )
            metadata_store_config = metadata_store.config

            alias_store: AliasStore = create_new_archive(  # type: ignore
                archive_name=archive_name,
                store_base_path=archive_base_path,
                store_type="sqlite_alias_store",
                file_name=archive_file_name,
                allow_write_access=allow_write_access,
                set_archive_name_metadata=False,
            )
            alias_store_config = alias_store.config

            job_store: JobStore = create_new_archive(  # type: ignore
                archive_name=archive_name,
                store_base_path=archive_base_path,
                store_type="sqlite_job_store",
                file_name=archive_file_name,
                allow_write_access=allow_write_access,
                set_archive_name_metadata=False,
            )
            job_store_config = job_store.config

            kiarchive_id = data_store.archive_id
            assert alias_store.archive_id == kiarchive_id
            assert metadata_store.archive_id == kiarchive_id
            assert job_store.archive_id == kiarchive_id

            kiarchive = KiArchive(
                archive_id=kiarchive_id,
                archive_name=archive_name,
                archive_base_path=archive_base_path,
                archive_file_name=archive_file_name,
                metadata_archive_config=metadata_store_config.model_dump(),
                data_archive_config=data_store_config.model_dump(),
                alias_archive_config=alias_store_config.model_dump(),
                job_archive_config=job_store_config.model_dump(),
                allow_write_access=allow_write_access,
            )
            kiarchive._metadata_archive = metadata_store
            kiarchive._data_archive = data_store
            kiarchive._alias_archive = alias_store
            kiarchive._jobs_archive = job_store
            kiarchive._kiara = kiara

        return kiarchive

    _kiara_model_id: ClassVar = "instance.kiarchive"

    archive_id: uuid.UUID = Field(description="The unique identifier of the archive.")
    archive_name: str = Field(description="The alias of the archive.")
    archive_base_path: str = Field(description="The base path/uri of the store.")
    archive_file_name: str = Field(description="The (file-)name of the store.")
    allow_write_access: bool = Field(
        description="Whether the store allows write access.", default=False
    )
    metadata_archive_config: Union[Mapping[str, Any], None] = Field(
        description="The archive to store metadata in.", default=None
    )
    data_archive_config: Union[Mapping[str, Any], None] = Field(
        description="The archive to store the data in.", default=None
    )
    alias_archive_config: Union[Mapping[str, Any], None] = Field(
        description="The archive to store aliases in.", default=None
    )
    job_archive_config: Union[Mapping[str, Any], None] = Field(
        description="The archive to store jobs in.", default=None
    )

    _metadata_archive: Union["MetadataArchive", None] = PrivateAttr(default=None)
    _data_archive: Union["DataArchive", None] = PrivateAttr(default=None)
    _alias_archive: Union["AliasArchive", None] = PrivateAttr(default=None)
    _jobs_archive: Union["JobArchive", None] = PrivateAttr(default=None)

    _kiara: Union["Kiara", None] = PrivateAttr(default=None)

    @property
    def metadata_archive(self) -> Union["MetadataArchive", None]:

        if self._metadata_archive:
            return self._metadata_archive

        if self.metadata_archive_config is None:
            return None

        from kiara.utils.stores import create_new_archive

        metadata_archive: MetadataArchive = create_new_archive(  # type: ignore
            archive_name=self.archive_name,
            store_base_path=self.archive_base_path,
            store_type="sqlite_metadata_store",
            file_name=self.archive_file_name,
            allow_write_access=True,
            **self.metadata_archive_config,
        )
        self._metadata_archive = metadata_archive
        return self._metadata_archive

    @property
    def data_archive(self) -> Union["DataArchive", None]:

        if self._data_archive:
            return self._data_archive

        if self.data_archive_config is None:
            return None

        from kiara.utils.stores import create_new_archive

        data_archive: DataArchive = create_new_archive(  # type: ignore
            archive_name=self.archive_name,
            store_base_path=self.archive_base_path,
            store_type="sqlite_data_store",
            file_name=self.archive_file_name,
            allow_write_access=True,
            **self.data_archive_config,
        )
        self._data_archive = data_archive
        return self._data_archive

    @property
    def alias_archive(self) -> Union["AliasArchive", None]:

        if self._alias_archive is not None:
            return self._alias_archive

        if self.alias_archive_config is None:
            return None

        from kiara.utils.stores import create_new_archive

        alias_archive: AliasStore = create_new_archive(  # type: ignore
            archive_name=self.archive_name,
            store_base_path=self.archive_base_path,
            store_type="sqlite_alias_store",
            file_name=self.archive_file_name,
            allow_write_access=True,
        )
        self._alias_archive = alias_archive
        return self._alias_archive

    @property
    def job_archive(self) -> Union["JobArchive", None]:

        if self._jobs_archive is not None:
            return self._jobs_archive

        if self.job_archive_config is None:
            return None

        from kiara.utils.stores import create_new_archive

        jobs_archive: JobStore = create_new_archive(  # type: ignore
            archive_name=self.archive_name,
            store_base_path=self.archive_base_path,
            store_type="sqlite_job_store",
            file_name=self.archive_file_name,
            allow_write_access=True,
        )
        self._jobs_archive = jobs_archive
        return self._jobs_archive
