# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from enum import Enum
from typing import Any, List, Mapping, Union

from pydantic import BaseModel, Field


class ByteProvisioningStrategy(Enum):

    INLINE = "INLINE"
    BYTES = "bytes"
    FILE_PATH_MAP = "link_map"
    LINK_FOLDER = "folder"
    COPIED_FOLDER = "copied_folder"


class BytesStructure(BaseModel):
    """A data structure that."""

    data_type: str = Field(description="The data type.")
    data_type_config: Mapping[str, Any] = Field(description="The data type config.")
    chunk_map: Mapping[str, List[Union[str, bytes]]] = Field(
        description="References to byte arrays, Keys are field names, values are a list of hash-ids that the data is composed of.",
        default_factory=dict,
    )

    # def provision_as_folder(self, copy_files: bool = False) -> Path:
    #     pass


class BytesAliasStructure(BaseModel):

    data_type: str = Field(description="The data type.")
    data_type_config: Mapping[str, Any] = Field(description="The data type config.")
    chunk_id_map: Mapping[str, List[str]] = Field(
        description="References to byte arrays, Keys are field names, values are a list of hash-ids that the data is composed of.",
        default_factory=dict,
    )
