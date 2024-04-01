# -*- coding: utf-8 -*-
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Literal,
    Mapping,
    Tuple,
    Union,
)

from pydantic import Field, field_validator

from kiara.defaults import (
    DEFAULT_DATA_STORE_MARKER,
    DEFAULT_METADATA_STORE_MARKER,
    DEFAULT_STORE_MARKER,
)
from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.events import RegistryEvent
from kiara.models.metadata import CommentMetadata, KiaraMetadata
from kiara.registries.metadata.metadata_store import MetadataArchive, MetadataStore

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.models.runtime_environment import RuntimeEnvironment


class MetadataMatcher(KiaraModel):
    """An object describing requirements metadata items should satisfy in order to be included in a query result."""

    @classmethod
    def create_matcher(cls, **match_options: Any):
        m = MetadataMatcher(**match_options)
        return m

    # metadata_item_keys: Union[None, List[str]] = Field(
    #     description="The metadata item key to match (if provided).", default=None
    # )
    reference_item_types: Union[None, List[str]] = Field(
        description="A 'reference_item_type' a metadata item is referenced from.",
        default=None,
    )
    reference_item_keys: Union[None, List[str]] = Field(
        description="A 'reference_item_key' a metadata item is referenced from.",
        default=None,
    )
    reference_item_ids: Union[None, List[str]] = Field(
        description="An list of ids that a metadata item is referenced from.",
        default=None,
    )

    @field_validator(
        "reference_item_types",
        "reference_item_keys",
        "reference_item_ids",
        mode="before",
    )
    @classmethod
    def validate_reference_item_ids(cls, v):

        if v is None:
            return None
        elif isinstance(v, str):
            return [v]
        elif isinstance(v, uuid.UUID):
            return [str(v)]
        else:
            v = set(v)
            result = [str(x) for x in v]
            return result


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
        self._default_metadata_store: Union[str, None] = None

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

            if set_as_default_store and self._default_metadata_store is not None:
                raise Exception(
                    f"Can't set data store '{alias}' as default store: default store already set."
                )

            if self._default_metadata_store is None or set_as_default_store:
                is_default_store = True
                self._default_metadata_store = alias

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
    def default_metadata_store(self) -> str:
        if self._default_metadata_store is None:
            raise Exception("No default metadata store set.")
        return self._default_metadata_store

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
            DEFAULT_DATA_STORE_MARKER,
        ):
            archive_id_or_alias = self.default_metadata_store
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

    def find_metadata_items(
        self, matcher: MetadataMatcher
    ) -> Generator[Tuple[Any, ...], None, None]:

        mounted_store: MetadataArchive = self.get_archive()

        return mounted_store.find_matching_metadata_items(matcher=matcher)

    def retrieve_environment_item(self, env_cid: str) -> "RuntimeEnvironment":

        if self._kiara.environment_registry.has_environment(env_cid):
            environment = self._kiara.environment_registry.get_environment_for_cid(
                env_cid
            )
        else:
            _environment = self.retrieve_metadata_item_with_hash(item_hash=env_cid)
            if _environment is None:
                raise KiaraException(
                    f"No environment with id '{env_cid}' available in metadata store."
                )

            from kiara.models.runtime_environment import RuntimeEnvironment

            if isinstance(_environment, RuntimeEnvironment):
                environment = _environment
            else:
                raise KiaraException(
                    f"Invalid environment item with id '{env_cid}' available in metadata store."
                )

        return environment

    def retrieve_metadata_item_with_hash(
        self, item_hash: str, store: Union[str, uuid.UUID, None] = None
    ) -> Union[KiaraMetadata, None]:
        """Retrieves a metadata item by its hash."""

        if store:
            mounted_archive: MetadataStore = self.get_archive(archive_id_or_alias=store)  # type: ignore
            result = mounted_archive.find_metadata_item_with_hash(item_hash=item_hash)
        else:
            mounted_archive: MetadataStore = self.get_archive(archive_id_or_alias=store)  # type: ignore
            result = mounted_archive.find_metadata_item_with_hash(item_hash=item_hash)
            if not result:
                for archive in self.metadata_archives.values():

                    result = archive.find_metadata_item_with_hash(item_hash=item_hash)
                    if result:
                        break

        if result is None:
            return None

        model_type_id, data = result
        model_cls = self._kiara.kiara_model_registry.get_model_cls(
            kiara_model_id=model_type_id, required_subclass=KiaraMetadata
        )

        model_instance = model_cls(**data)
        return model_instance  # type: ignore

    def retrieve_metadata_item(
        self,
        key: str,
        reference_item_type: Union[str, None] = None,
        reference_item_key: Union[str, None] = None,
        reference_item_id: Union[str, None] = None,
        store: Union[str, uuid.UUID, None] = None,
    ) -> Union[KiaraMetadata, None]:
        """Retrieves a metadata item."""

        if store:
            mounted_store: MetadataStore = self.get_archive(archive_id_or_alias=store)  # type: ignore
            result = mounted_store.retrieve_metadata_item(
                metadata_item_key=key,
                reference_type=reference_item_type,
                reference_key=reference_item_key,
                reference_id=reference_item_id,
            )
        else:
            mounted_store: MetadataStore = self.get_archive(archive_id_or_alias=store)  # type: ignore
            result = mounted_store.retrieve_metadata_item(
                metadata_item_key=key,
                reference_type=reference_item_type,
                reference_key=reference_item_key,
                reference_id=reference_item_id,
            )
            if not result:

                for archive in self.metadata_archives.values():
                    result = archive.retrieve_metadata_item(
                        metadata_item_key=key,
                        reference_type=reference_item_type,
                        reference_key=reference_item_key,
                        reference_id=reference_item_id,
                    )
                    if result:
                        break

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
        reference_item_key: Union[str, None] = None,
        reference_item_id: Union[str, None] = None,
        replace_existing_references: bool = False,
        allow_multiple_references: bool = False,
        store: Union[str, uuid.UUID, None] = None,
    ) -> uuid.UUID:

        mounted_store: MetadataStore = self.get_archive(archive_id_or_alias=store)  # type: ignore

        result = mounted_store.store_metadata_item(
            key=key,
            item=item,
            reference_item_type=reference_item_type,
            reference_item_key=reference_item_key,
            reference_item_id=reference_item_id,
            replace_existing_references=replace_existing_references,
            allow_multiple_references=allow_multiple_references,
        )
        return result

    def register_job_metadata_items(
        self,
        job_id: uuid.UUID,
        items: Mapping[str, Any],
        store: Union[str, uuid.UUID, None] = None,
        reference_item_key: Union[str, None] = None,
        replace_existing_references: bool = True,
        allow_multiple_references: bool = False,
    ) -> None:

        for key, value in items.items():

            _reference_item_key = None
            if isinstance(value, str):
                value = CommentMetadata(comment=value)
                if not reference_item_key:
                    _reference_item_key = "comment"
                else:
                    _reference_item_key = reference_item_key
            elif isinstance(value, CommentMetadata):
                _reference_item_key = "comment"
            elif not isinstance(value, KiaraMetadata):
                raise Exception(f"Invalid metadata value for key '{key}': {value}")

            if not _reference_item_key:
                _reference_item_key = value._kiara_model_id

            self.register_metadata_item(
                key=key,
                item=value,
                reference_item_type="job",
                reference_item_key=_reference_item_key,
                reference_item_id=str(job_id),
                store=store,
                replace_existing_references=replace_existing_references,
                allow_multiple_references=allow_multiple_references,
            )

    def retrieve_job_metadata_items(self, job_id: uuid.UUID):

        pass

    def retrieve_job_metadata_item(
        self, job_id: uuid.UUID, key: str, store: Union[str, uuid.UUID, None] = None
    ) -> Union[KiaraMetadata, None]:

        return self.retrieve_metadata_item(
            key=key,
            reference_item_type="job",
            reference_item_key="comment",
            reference_item_id=str(job_id),
            store=store,
        )
