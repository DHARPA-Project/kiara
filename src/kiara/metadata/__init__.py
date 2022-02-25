# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from pydantic import BaseModel, Field
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.info import KiaraInfoModel, extract_renderable

if typing.TYPE_CHECKING:
    from kiara import Kiara
    from kiara.metadata.core_models import MetadataModelMetadata, PythonClassMetadata


class MetadataModel(KiaraInfoModel):
    """Base class for classes that represent value metadata in kiara."""

    # @classmethod
    # def get_type_metadata(cls) -> "MetadataModelMetadata":
    #     """Return all metadata associated with this module type."""
    #
    #     from kiara.metadata.core_models import MetadataModelMetadata
    #
    #     return MetadataModelMetadata.from_model_class(model_cls=cls)

    def get_category_alias(self) -> str:
        return "metadata"

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for k in self.__fields__.keys():
            if k == "type_name" and config.get("omit_type_name", False):
                continue
            attr = getattr(self, k)
            v = extract_renderable(attr)
            table.add_row(k, v)

        if "operations" in config.keys():
            ids = list(config["operations"].keys())
            table.add_row("operations", "\n".join(ids))

        return table


class WrapperMetadataModel(MetadataModel):

    python_class: "PythonClassMetadata" = Field(
        description="Information about the Python class for this module type."
    )

    def get_id(self) -> str:
        return self.python_class.get_id()


class MetadataSet(object):
    def __init__(
        self,
        kiara: "Kiara",
        subject: typing.Any,
        **metadata: typing.Mapping[str, typing.Any],
    ):

        self._kiara: "Kiara" = kiara
        self._data: typing.Dict[str, typing.Mapping[str, typing.Any]] = metadata
        self._metadata: typing.Dict[str, MetadataModel] = {}
        self._subject: typing.Any = subject

    def get_metadata(self, metadata_key: str) -> MetadataModel:

        if metadata_key in self._metadata.keys():
            return self._metadata[metadata_key]

        if metadata_key not in self._data.keys():
            raise Exception(f"No metadata for key '{metadata_key}' available.")

        md = self._data[metadata_key]
        schema = self._kiara.metadata_mgmt.all_schemas.get(metadata_key, None)
        if schema is None:
            raise Exception(
                f"No metadata schema for key '{metadata_key}' registered in kiara."
            )

        metadata_model_obj = schema(**md)
        self._metadata[metadata_key] = metadata_model_obj
        return self._metadata[metadata_key]

    def get_schema(self, metadata_key) -> "MetadataModelMetadata":
        pass


class ValueTypeAndDescription(BaseModel):

    description: str = Field(description="The description for the value.")
    type: str = Field(description="The value type.")
    value_default: typing.Any = Field(
        description="Default for the value.", default=None
    )
    required: bool = Field(description="Whether this value is required")
