# -*- coding: utf-8 -*-
import abc
import json
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Generic,
    Iterable,
    Mapping,
    Tuple,
    Union,
)

from kiara.exceptions import KiaraException
from kiara.models.metadata import KiaraMetadata
from kiara.registries import ARCHIVE_CONFIG_CLS, BaseArchive

if TYPE_CHECKING:
    from kiara.registries.metadata import MetadataMatcher


class MetadataArchive(BaseArchive[ARCHIVE_CONFIG_CLS], Generic[ARCHIVE_CONFIG_CLS]):
    """Base class for data archiv implementationss."""

    @classmethod
    def supported_item_types(cls) -> Iterable[str]:
        """This archive type only supports storing data."""

        return ["metadata"]

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
        self._schema_stored_cache: Dict[str, Any] = {}
        self._schema_stored_item: Dict[str, uuid.UUID] = {}

    def find_metadata_item_with_hash(
        self, item_hash: str, key: Union[str, None] = None
    ) -> Union[Tuple[str, Mapping[str, Any]], None]:
        """Return the key of the metadata item with the specified hash."""

        return self._retrieve_metadata_item_with_hash(item_hash=item_hash, key=key)

    @abc.abstractmethod
    def _retrieve_metadata_item_with_hash(
        self, item_hash: str, key: Union[str, None] = None
    ) -> Union[Tuple[str, Mapping[str, Any]], None]:
        pass

    def find_matching_metadata_items(
        self,
        matcher: "MetadataMatcher",
        metadata_item_result_fields: Union[Iterable[str], None] = None,
        reference_item_result_fields: Union[Iterable[str], None] = None,
    ) -> Generator[Tuple[Any, ...], None, None]:

        return self._find_matching_metadata_and_ref_items(
            matcher=matcher,
            metadata_item_result_fields=metadata_item_result_fields,
            reference_item_result_fields=reference_item_result_fields,
        )

    @abc.abstractmethod
    def _find_matching_metadata_and_ref_items(
        self,
        matcher: "MetadataMatcher",
        metadata_item_result_fields: Union[Iterable[str], None] = None,
        reference_item_result_fields: Union[Iterable[str], None] = None,
    ) -> Generator[Tuple[Any, ...], None, None]:
        pass

    def retrieve_metadata_item(
        self,
        metadata_item_key: str,
        reference_type: Union[str, None] = None,
        reference_key: Union[str, None] = None,
        reference_id: Union[str, None] = None,
    ) -> Union[Tuple[str, Mapping[str, Any]], None]:
        """Return the model type and model data for the specified metadata item.

        If more than one item matches, an exception is raised.

        Arguments:
            metadata_item_key: The key of the metadata item to retrieve.
            reference_type: The type of the referenced item.
            reference_key: The key of the referenced item.
            reference_id: The id of the referenced item.
        """

        if reference_id and (not reference_type or not reference_key):
            raise ValueError(
                "If reference_id is set, reference_key & reference_type must be set as well."
            )
        if reference_type and reference_key:
            if reference_id is None:
                raise KiaraException(
                    msg="reference_id must set also if reference_type is set."
                )
            result = self._retrieve_referenced_metadata_item_data(
                key=metadata_item_key,
                reference_type=reference_type,
                reference_key=reference_key,
                reference_id=reference_id,
            )
            if result is None:
                return None
            else:
                return result
        else:
            raise NotImplementedError(
                "Retrieving metadata item without reference not implemented yet."
            )

    @abc.abstractmethod
    def _retrieve_referenced_metadata_item_data(
        self, key: str, reference_type: str, reference_key: str, reference_id: str
    ) -> Union[Tuple[str, Mapping[str, Any]], None]:
        """Return the model type id and model data for the specified referenced metadata item."""


class MetadataStore(MetadataArchive):
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

    @classmethod
    def _is_writeable(cls) -> bool:
        return True

    @abc.abstractmethod
    def _store_metadata_schema(
        self, model_schema_hash: str, model_type_id: str, model_schema: str
    ):
        """Store the metadata schema for the specified model."""

    def store_metadata_item(
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
        """Store a metadata item into the store.

        If `reference_item_type` and `reference_item_id` are set, the stored metadata item will
        be linked to the stored metadata item, to enable lokoups later on.
        """

        if store:
            raise NotImplementedError(
                "Cannot store metadata item with store, not implemented yet."
            )

        # TODO: check if already stored
        model_type = item.model_type_id
        model_schema_hash = str(item.get_schema_cid())

        if model_schema_hash not in self._schema_stored_cache.keys():

            model_item_schema = item.model_json_schema()
            model_item_schema_str = json.dumps(model_item_schema)

            self._store_metadata_schema(
                model_schema_hash=model_schema_hash,
                model_type_id=model_type,
                model_schema=model_item_schema_str,
            )
            self._schema_stored_cache[model_schema_hash] = model_item_schema

        # data = item.model_dump()
        data_json = item.model_dump_json()
        data_hash = str(item.instance_cid)

        metadata_item_id = self._schema_stored_item.get(data_hash, None)
        if not metadata_item_id:

            metadata_item_id = self._store_metadata_item(
                key=key,
                value_json=data_json,
                value_hash=data_hash,
                model_type_id=model_type,
                model_schema_hash=model_schema_hash,
            )
            self._schema_stored_item[data_hash] = metadata_item_id

        if (reference_item_id and not reference_item_type) or (
            reference_item_type and not reference_item_id
        ):
            raise ValueError(
                "If reference_item_id is set, reference_item_type must be set as well."
            )

        if reference_item_type:
            assert reference_item_id is not None
            assert reference_item_key is not None
            self._store_metadata_reference(
                reference_item_type=reference_item_type,
                reference_item_key=reference_item_key,
                reference_item_id=reference_item_id,
                metadata_item_id=str(metadata_item_id),
                replace_existing_references=replace_existing_references,
                allow_multiple_references=allow_multiple_references,
            )

        return metadata_item_id

    @abc.abstractmethod
    def _store_metadata_reference(
        self,
        reference_item_type: str,
        reference_item_key: str,
        reference_item_id: str,
        metadata_item_id: str,
        replace_existing_references: bool = False,
        allow_multiple_references: bool = False,
    ) -> None:
        pass

    @abc.abstractmethod
    def _store_metadata_item(
        self,
        key: str,
        value_json: str,
        value_hash: str,
        model_type_id: str,
        model_schema_hash: str,
    ) -> uuid.UUID:
        pass

    def store_metadata_and_ref_items(
        self, items: Generator[Tuple[Any, ...], None, None]
    ):

        return self._store_metadata_and_ref_items(items)

    @abc.abstractmethod
    def _store_metadata_and_ref_items(
        self, items: Generator[Tuple[Any, ...], None, None]
    ):

        pass
