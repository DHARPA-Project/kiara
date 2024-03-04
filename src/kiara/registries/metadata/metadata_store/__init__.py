# -*- coding: utf-8 -*-
import abc
from typing import Any, Generic, Iterable, Union

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
    @classmethod
    def _is_writeable(cls) -> bool:
        return True

    @abc.abstractmethod
    def store_metadata(self, key: str, value: Any):
        pass
