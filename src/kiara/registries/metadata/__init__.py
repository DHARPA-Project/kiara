# -*- coding: utf-8 -*-
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, Literal, Mapping, Union

from pydantic import Field

from kiara.defaults import DEFAULT_METADATA_STORE_MARKER, DEFAULT_STORE_MARKER
from kiara.models.events import RegistryEvent
from kiara.models.metadata import CommentMetadata, KiaraMetadata
from kiara.registries.metadata.metadata_store import MetadataArchive, MetadataStore

if TYPE_CHECKING:
    from kiara.context import Kiara


class MetadataArchiveAddedEvent(RegistryEvent):

    event_type: Literal["metadata_archive_added"] = "metadata_archive_added"
    metadata_archive_id: uuid.UUID = Field(
        description="The unique id of this metadata archive."
    )
    metadata_archive_alias: str = Field(
        description="The alias this metadata archive was added as."
    )
    is_store: bool = Field(
        description="Whether this archive supports write operations (aka implements the 'MetadataStore' interface)."
    )
    is_default_store: bool = Field(
        description="Whether this store acts as default store."
    )


class MetadataRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._event_callback: Callable = self._kiara.event_registry.add_producer(self)

        self._metadata_archives: Dict[str, MetadataArchive] = {}
        self._default_data_store: Union[str, None] = None

        # self._env_registry: EnvironmentRegistry = self._kiara.environment_registry

    @property
    def kiara_id(self) -> uuid.UUID:
        return self._kiara.id

    def register_metadata_archive(
        self,
        archive: MetadataArchive,
        set_as_default_store: Union[bool, None] = None,
    ) -> str:

        alias = archive.archive_name

        if not alias:
            raise Exception("Invalid data archive alias: can't be empty.")

        if alias in self._metadata_archives.keys():
            raise Exception(
                f"Can't add data archive, alias '{alias}' already registered."
            )

        archive.register_archive(kiara=self._kiara)

        self._metadata_archives[alias] = archive
        is_store = False
        is_default_store = False
        if isinstance(archive, MetadataStore):
            is_store = True

            if set_as_default_store and self._default_data_store is not None:
                raise Exception(
                    f"Can't set data store '{alias}' as default store: default store already set."
                )

            if self._default_data_store is None or set_as_default_store:
                is_default_store = True
                self._default_data_store = alias

        event = MetadataArchiveAddedEvent(
            kiara_id=self._kiara.id,
            metadata_archive_id=archive.archive_id,
            metadata_archive_alias=alias,
            is_store=is_store,
            is_default_store=is_default_store,
        )
        self._event_callback(event)

        return alias

    @property
    def default_data_store(self) -> str:
        if self._default_data_store is None:
            raise Exception("No default metadata store set.")
        return self._default_data_store

    @property
    def metadata_archives(self) -> Mapping[str, MetadataArchive]:
        return self._metadata_archives

    def get_archive(
        self, archive_id_or_alias: Union[None, uuid.UUID, str] = None
    ) -> MetadataArchive:

        if archive_id_or_alias in (
            None,
            DEFAULT_STORE_MARKER,
            DEFAULT_METADATA_STORE_MARKER,
        ):
            archive_id_or_alias = self.default_data_store
            if archive_id_or_alias is None:
                raise Exception(
                    "Can't retrieve default metadata archive, none set (yet)."
                )

        if isinstance(archive_id_or_alias, uuid.UUID):
            for archive in self._metadata_archives.values():
                if archive.archive_id == archive_id_or_alias:
                    return archive

            raise Exception(
                f"Can't retrieve metadata archive with id '{archive_id_or_alias}': no archive with that id registered."
            )

        if archive_id_or_alias in self._metadata_archives.keys():
            return self._metadata_archives[archive_id_or_alias]
        else:
            try:
                _archive_id = uuid.UUID(archive_id_or_alias)
                for archive in self._metadata_archives.values():
                    if archive.archive_id == _archive_id:
                        return archive
                    raise Exception(
                        f"Can't retrieve archive with id '{archive_id_or_alias}': no archive with that id registered."
                    )
            except Exception:
                pass

        raise Exception(
            f"Can't retrieve archive with id '{archive_id_or_alias}': no archive with that id registered."
        )

    def retrieve_metadata_item(
        self,
        key: str,
        reference_item_type: Union[str, None] = None,
        reference_item_id: Union[str, None] = None,
        store: Union[str, uuid.UUID, None] = None,
    ) -> Union[KiaraMetadata, None]:
        """Retrieves a metadata item."""

        mounted_store: MetadataStore = self.get_archive(archive_id_or_alias=store)  # type: ignore

        result = mounted_store.retrieve_metadata_item(
            key=key, reference_type=reference_item_type, reference_id=reference_item_id
        )

        if result is None:
            return None

        model_type_id, data = result
        model_cls = self._kiara.kiara_model_registry.get_model_cls(
            kiara_model_id=model_type_id, required_subclass=KiaraMetadata
        )

        model_instance = model_cls(**data)
        return model_instance  # type: ignore

    def register_metadata_item(
        self,
        key: str,
        item: KiaraMetadata,
        reference_item_type: Union[str, None] = None,
        reference_item_id: Union[str, None] = None,
        force: bool = False,
        store: Union[str, uuid.UUID, None] = None,
    ) -> uuid.UUID:

        mounted_store: MetadataStore = self.get_archive(archive_id_or_alias=store)  # type: ignore

        return mounted_store.store_metadata_item(
            key=key,
            item=item,
            reference_item_type=reference_item_type,
            reference_item_id=reference_item_id,
            force=force,
        )

    def register_job_metadata_items(
        self,
        job_id: uuid.UUID,
        items: Mapping[str, Any],
        store: Union[str, uuid.UUID, None] = None,
        force: bool = False,
    ) -> None:

        for key, value in items.items():
            if isinstance(value, str):
                value = CommentMetadata(comment=value)
            elif not isinstance(value, KiaraMetadata):
                raise Exception(f"Invalid metadata value for key '{key}': {value}")
            self.register_metadata_item(
                key=key,
                item=value,
                reference_item_type="job",
                reference_item_id=str(job_id),
                store=store,
                force=force,
            )

    def retrieve_job_metadata_items(self, job_id: uuid.UUID):

        pass

    def retrieve_job_metadata_item(
        self, job_id: uuid.UUID, key: str, store: Union[str, uuid.UUID, None] = None
    ) -> Union[KiaraMetadata, None]:

        return self.retrieve_metadata_item(
            key=key,
            reference_item_type="job",
            reference_item_id=str(job_id),
            store=store,
        )
