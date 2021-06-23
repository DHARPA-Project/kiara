# -*- coding: utf-8 -*-
import inspect
import json
import textwrap
import typing
from pydantic import BaseModel, Field
from pydantic.schema import (
    get_flat_models_from_model,
    get_model_name_map,
    model_process_schema,
)
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from kiara.data.values import ValueSchema
from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.metadata import MetadataModel
from kiara.metadata.core_models import (
    ContextMetadataModel,
    DocumentationMetadataModel,
    OriginMetadataModel,
    PythonClassMetadata,
)
from kiara.module_config import KiaraModuleConfig, PipelineModuleConfig
from kiara.utils import create_table_from_field_schemas

if typing.TYPE_CHECKING:
    from kiara import KiaraModule


class ValueTypeAndDescription(BaseModel):

    description: str = Field(description="The description for the value.")
    type: str = Field(description="The value type.")


class KiaraModuleConfigMetadata(MetadataModel):
    @classmethod
    def from_config_class(
        cls,
        config_cls: typing.Type[KiaraModuleConfig],
        remove_pipeline_config: bool = False,
    ):

        flat_models = get_flat_models_from_model(config_cls)
        model_name_map = get_model_name_map(flat_models)
        m_schema, _, _ = model_process_schema(config_cls, model_name_map=model_name_map)
        fields = m_schema["properties"]

        config_values = {}
        for field_name, details in fields.items():
            if remove_pipeline_config and field_name in [
                "steps",
                "input_aliases",
                "output_aliases",
                "doc",
            ]:
                continue

            type_str = "-- n/a --"
            if "type" in details.keys():
                type_str = details["type"]

            desc = details.get("description", DEFAULT_NO_DESC_VALUE)
            config_values[field_name] = ValueTypeAndDescription(
                description=desc, type=type_str
            )

        python_cls = PythonClassMetadata.from_class(config_cls)
        return KiaraModuleConfigMetadata(
            python_class=python_cls, config_values=config_values
        )

    python_class: PythonClassMetadata = Field(
        description="The Python class for this configuration."
    )
    config_values: typing.Dict[str, ValueTypeAndDescription] = Field(
        description="The available configuration values."
    )


class KiaraModuleTypeMetadata(MetadataModel):
    @classmethod
    def from_module_class(cls, module_cls: typing.Type["KiaraModule"]):

        proc_src = textwrap.dedent(inspect.getsource(module_cls.process))

        origin_md = OriginMetadataModel.from_class(module_cls)
        properties_md = ContextMetadataModel.from_class(module_cls)
        doc = DocumentationMetadataModel.from_class_doc(module_cls)

        python_class = PythonClassMetadata.from_class(module_cls)
        config = KiaraModuleConfigMetadata.from_config_class(module_cls._config_cls)
        is_pipeline = module_cls.is_pipeline()
        pipeline_config = None
        if is_pipeline:
            pipeline_config = module_cls._base_pipeline_config  # type: ignore

        return cls(type_name=module_cls._module_type_name, documentation=doc, origin=origin_md, context=properties_md, python_class=python_class, config=config, is_pipeline=is_pipeline, pipeline_config=pipeline_config, process_src=proc_src)  # type: ignore

    type_name: str = Field(description="The registered name for this module type.")
    documentation: DocumentationMetadataModel = Field(
        description="Documentation for the module."
    )
    origin: OriginMetadataModel = Field(
        description="Information about authorship for the module type."
    )
    context: ContextMetadataModel = Field(
        description="Generic properties of this module (description, tags, labels, references, ...)."
    )
    python_class: PythonClassMetadata = Field(
        description="Information about the Python class for this module type."
    )
    config: KiaraModuleConfigMetadata = Field(
        description="Details on how this module type can be configured."
    )
    is_pipeline: bool = Field(
        description="Whether the module type is a pipeline, or a core module."
    )
    pipeline_config: typing.Optional[PipelineModuleConfig] = Field(
        description="If this module is a pipeline, this field contains the pipeline configuration.",
        default_factory=None,
    )
    process_src: str = Field(
        description="The source code of the process method of the module."
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        include_config = config.get("include_config", True)
        include_doc = config.get("include_doc", True)

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
        table.add_row("Python class", self.python_class.create_renderable())

        if include_config:
            if self.is_pipeline:
                json_str = self.pipeline_config.json(indent=2)  # type: ignore
                _config: Syntax = Syntax(json_str, "json", background_color="default")
                table.add_row("Pipeline config", Panel(_config, box=box.HORIZONTALS))
            else:
                _config = Syntax(self.process_src, "python", background_color="default")
                table.add_row(
                    "Processing source code", Panel(_config, box=box.HORIZONTALS)
                )

        return table


class KiaraModuleInstanceMetadata(MetadataModel):
    @classmethod
    def from_module_obj(cls, obj: "KiaraModule"):

        config = obj.config.dict()
        for x in [
            "steps",
            "input_aliases",
            "output_aliases",
            "module_type_name",
            "doc",
            "metadata",
        ]:
            config.pop(x, None)

        type_metadata = KiaraModuleTypeMetadata.from_module_class(obj.__class__)
        result = KiaraModuleInstanceMetadata(
            type_metadata=type_metadata,
            config=config,
            inputs_schema=obj.input_schemas,
            outputs_schema=obj.output_schemas,
        )
        return result

    type_metadata: KiaraModuleTypeMetadata = Field(
        description="Metadata for the module type of this instance."
    )
    config: typing.Dict[str, typing.Any] = Field(
        description="Configuration that was used to create this module instance."
    )
    inputs_schema: typing.Dict[str, ValueSchema] = Field(
        description="The schema for the module inputs."
    )
    outputs_schema: typing.Dict[str, ValueSchema] = Field(
        description="The schema for the module outputs."
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(box=box.SIMPLE, show_header=False, show_lines=True)
        table.add_column("Property", style="i")
        table.add_column("Value")

        table.add_row("Description", self.type_metadata.documentation.description)

        table.add_row(
            "Type information",
            self.type_metadata.create_renderable(
                include_config=False, include_doc=False
            ),
        )
        conf = Syntax(
            json.dumps(self.config, indent=2), "json", background_color="default"
        )
        table.add_row("Configuration", conf)

        constants = self.config.get("constants")
        inputs_table = create_table_from_field_schemas(
            _add_required=True,
            _add_default=True,
            _show_header=True,
            _constants=constants,
            **self.inputs_schema,
        )
        table.add_row("Inputs", inputs_table)
        outputs_table = create_table_from_field_schemas(
            _add_required=False,
            _add_default=False,
            _show_header=True,
            _constants=None,
            **self.outputs_schema,
        )
        table.add_row("Outputs", outputs_table)

        return table
