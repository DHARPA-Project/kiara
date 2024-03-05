# -*- coding: utf-8 -*-
import abc
import json
import uuid
from typing import Any, Dict, Generic, Iterable, Mapping, Union

from kiara.models.metadata import KiaraMetadata
from kiara.registries import ARCHIVE_CONFIG_CLS, BaseArchive


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

    def retrieve_metadata_value(
        self,
        key: str,
        metadata_model: Union[str, None] = None,
        reference_id: Union[str, None] = None,
    ) -> Any:

        pass


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
        self._schema_stored_cache: Dict[str, Any] = {}

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
        reference_item: Any = None,
        store: Union[str, uuid.UUID, None] = None,
    ):

        if reference_item:
            raise NotImplementedError(
                "Cannot store metadata item with reference item, not implemented yet."
            )

        if store:
            raise NotImplementedError(
                "Cannot store metadata item with store, not implemented yet."
            )

        # TODO: check if already stored
        model_type = item.model_type_id
        model_schema_hash = str(item.get_schema_cid())
        model_item_schema = item.model_json_schema()
        model_item_schema_str = json.dumps(model_item_schema)

        self._store_metadata_schema(
            model_schema_hash=model_schema_hash,
            model_type_id=model_type,
            model_schema=model_item_schema_str,
        )

        data = item.model_dump()
        data_hash = str(item.instance_cid)

        self._store_metadata_item(
            key=key,
            value=data,
            value_hash=data_hash,
            model_type_id=model_type,
            model_schema_hash=model_schema_hash,
        )

    @abc.abstractmethod
    def _store_metadata_item(
        self,
        key: str,
        value: Mapping[str, Any],
        value_hash: str,
        model_type_id: str,
        model_schema_hash: str,
    ):
        pass
