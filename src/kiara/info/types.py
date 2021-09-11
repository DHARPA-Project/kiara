# -*- coding: utf-8 -*-
import typing
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.table import Table

from kiara import Kiara
from kiara.data.types import ValueTypeConfigMetadata
from kiara.info import KiaraInfoModel
from kiara.metadata import MetadataModel
from kiara.metadata.core_models import (
    ContextMetadataModel,
    DocumentationMetadataModel,
    MetadataModelMetadata,
    OriginMetadataModel,
    PythonClassMetadata,
)
from kiara.utils.output import create_table_from_base_model

if typing.TYPE_CHECKING:
    from kiara.data.types import ValueType


class ValueTypeInfo(KiaraInfoModel):
    @classmethod
    def from_type_class(
        cls, type_cls: typing.Type["ValueType"], kiara: typing.Optional[Kiara] = None
    ):

        type_attrs = cls.extract_type_attributes(type_cls=type_cls, kiara=kiara)
        return cls(**type_attrs)

    @classmethod
    def extract_type_attributes(
        self, type_cls: typing.Type["ValueType"], kiara: typing.Optional[Kiara] = None
    ) -> typing.Dict[str, typing.Any]:

        if kiara is None:
            kiara = Kiara.instance()

        origin_md = OriginMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClassMetadata.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)

        value_type: str = type_cls._value_type_name  # type: ignore
        config = ValueTypeConfigMetadata.from_config_class(type_cls._config_class)

        metadata_keys = kiara.metadata_mgmt.get_metadata_keys_for_type(
            value_type=value_type
        )
        metadata_schemas: typing.Dict[str, typing.Type[MetadataModel]] = {}
        for metadata_key in metadata_keys:
            schema = kiara.metadata_mgmt.all_schemas.get(metadata_key)
            if schema is not None:
                metadata_schemas[metadata_key] = MetadataModelMetadata.from_model_class(
                    schema
                )

        return {
            "type_name": type_cls._value_type_name,  # type: ignore
            "documentation": doc,
            "origin": origin_md,
            "context": properties_md,
            "python_class": python_class,
            "config": config,
            "metadata_types": metadata_schemas,
        }

    type_name: str = Field(description="The name under which the type is registered.")
    documentation: DocumentationMetadataModel = Field(
        description="The documentation for this value type."
    )
    origin: OriginMetadataModel = Field(
        description="Information about authorship for the value type."
    )
    context: ContextMetadataModel = Field(
        description="Generic properties of this value type (description, tags, labels, references, ...)."
    )
    python_class: PythonClassMetadata = Field(
        description="Information about the Python class for this value type."
    )
    config: ValueTypeConfigMetadata = Field(
        description="Details on how this value type can be configured."
    )
    metadata_types: typing.Dict[str, MetadataModelMetadata] = Field(
        description="The available metadata types for this value type."
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        include_config_schema = config.get("include_config_schema", True)
        include_doc = config.get("include_doc", True)
        include_full_metadata = config.get("include_full_metadata", True)

        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
        table.add_column("property", style="i")
        table.add_column("value")

        if include_doc:
            table.add_row(
                "Documentation",
                Panel(self.documentation.create_renderable(), box=box.SIMPLE),
            )
        table.add_row("Origin", self.origin.create_renderable())
        table.add_row("Context", self.context.create_renderable())

        if include_config_schema:
            if config:
                config_cls = self.config.python_class.get_class()
                table.add_row("Type config", create_table_from_base_model(config_cls))
            else:
                table.add_row("Type config", "-- no config --")

        table.add_row("Python class", self.python_class.create_renderable())

        if include_full_metadata and self.metadata_types:
            md_table = Table(show_header=False, box=box.SIMPLE)
            for key, md in self.metadata_types.items():
                fields_table = md.create_fields_table(
                    show_header=False, show_required=False
                )
                md_table.add_row(f"[i]{key}[/i]", fields_table)

            table.add_row("Type metadata", md_table)

        return table
