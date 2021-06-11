# -*- coding: utf-8 -*-
import typing
from pydantic import AnyUrl, BaseModel, Field

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.metadata import MetadataModel


class PythonClassMetadata(MetadataModel):
    class_name: str = Field(description="The name of the Python class")
    module_name: str = Field(
        description="The name of the Python module this class lives in."
    )
    full_name: str = Field(description="The full class namespace.")


class LinkModel(BaseModel):

    url: AnyUrl = Field(description="The url.")
    desc: typing.Optional[str] = Field(
        description="A short description of the link content.",
        default=DEFAULT_NO_DESC_VALUE,
    )


class BaseMetadataModel(MetadataModel):

    description: str = Field(
        description="Description of the item (in markdown).",
        default=DEFAULT_NO_DESC_VALUE,
    )
    references: typing.Dict[str, LinkModel] = Field(
        description="References for the item.", default_factory=dict
    )
    tags: typing.List[str] = Field(
        description="A list of tags for the item.", default_factory=list
    )
    labels: typing.Dict[str, str] = Field(
        description="A list of labels for the item.", default_factory=list
    )


class KiaraModuleTypeMetadata(BaseMetadataModel):

    is_pipeline: bool = Field(
        description="Whether the module type is a pipeline, or a core module."
    )