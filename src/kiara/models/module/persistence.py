# -*- coding: utf-8 -*-
from enum import Enum
from pydantic import BaseModel, Field
from typing import Any, Iterable, Mapping, Union

from kiara.models.module.manifest import Manifest


class ByteProvisioningStrategy(Enum):

    INLINE = "INLINE"
    BYTES = "bytes"
    LINK_MAP = "link_map"
    LINK_FOLDER = "folder"
    COPIED_FOLDER = "copied_folder"


class BytesStructure(BaseModel):
    """A data structure that"""

    data_type: str = Field(description="The data type.")
    data_type_config: Mapping[str, Any] = Field(description="The data type config.")
    bytes_map: Mapping[str, Iterable[Union[str, bytes]]] = Field(
        description="References to byte arrays, Keys are field names, values are a list of hash-ids that the data is composed of."
    )


class LoadConfig(Manifest):

    provisioning_strategy: ByteProvisioningStrategy = Field(
        description="In what form the  serialized bytes are returned.",
        default_factory=ByteProvisioningStrategy.INLINE,
    )
    # bytes_structure: Optional[BytesStructure] = Field(
    #     description="A description of the bytes structure of the (serialized) data.",
    #     default=None,
    # )
    inputs: Mapping[str, str] = Field(
        description="A map translating from input field name to alias (key) in bytes_structure."
    )
    output_name: str = Field(
        description="The name of the field that contains the persisted value details."
    )

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    def __repr__(self):

        return f"{self.__class__.__name__}(module_type={self.module_type}, output_name={self.output_name})"

    def __str__(self):
        return self.__repr__()
