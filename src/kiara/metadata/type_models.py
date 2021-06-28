# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara.metadata import MetadataModel
from kiara.metadata.core_models import (
    ContextMetadataModel,
    DocumentationMetadataModel,
    OriginMetadataModel,
    PythonClassMetadata,
)

if typing.TYPE_CHECKING:
    from kiara.data.types import ValueType


class ValueTypeMetadata(MetadataModel):
    @classmethod
    def from_value_type_class(cls, value_type_cls: typing.Type["ValueType"]):

        origin_md = OriginMetadataModel.from_class(value_type_cls)
        doc = DocumentationMetadataModel.from_class_doc(value_type_cls)
        python_class = PythonClassMetadata.from_class(value_type_cls)
        properties_md = ContextMetadataModel.from_class(value_type_cls)

        return ValueTypeMetadata(
            type_name=value_type_cls._value_type_name,  # type: ignore
            documentation=doc,
            origin=origin_md,
            context=properties_md,
            python_class=python_class,
        )

    type_name: str = Field(description="The registered name for this value type.")
    documentation: DocumentationMetadataModel = Field(
        description="Documentation for the value type."
    )
    origin: OriginMetadataModel = Field(
        description="Information about the creator of this value type."
    )
    context: ContextMetadataModel = Field(
        description="Generic properties of this value type."
    )
    python_class: PythonClassMetadata = Field(
        description="The Python class for this value type."
    )
