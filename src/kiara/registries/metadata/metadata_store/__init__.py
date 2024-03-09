# -*- coding: utf-8 -*-
import abc
import json
import uuid
from typing import Any, Dict, Generic, Iterable, Union

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
        reference_item_type: Union[str, None] = None,
        reference_item_id: Union[str, None] = None,
        force: bool = False,
        store: Union[str, uuid.UUID, None] = None,
    ) -> uuid.UUID:

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

        # data = item.model_dump()
        data_json = item.model_dump_json()
        data_hash = str(item.instance_cid)

        metadata_item_id = self._store_metadata_item(
            key=key,
            value_json=data_json,
            value_hash=data_hash,
            model_type_id=model_type,
            model_schema_hash=model_schema_hash,
            force=force,
        )

        if (reference_item_id and not reference_item_type) or (
            reference_item_type and not reference_item_id
        ):
            raise ValueError(
                "If reference_item_id is set, reference_item_type must be set as well."
            )

        if reference_item_type:
            self._store_metadata_reference(
                reference_item_type, reference_item_id, str(metadata_item_id)
            )

        return metadata_item_id

    @abc.abstractmethod
    def _store_metadata_reference(
        self, reference_item_type: str, reference_item_id: str, metadata_item_id: str
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
        force: bool = False,
    ) -> uuid.UUID:
        pass
