# -*- coding: utf-8 -*-
import inspect
import json
import textwrap
import typing
from pydantic import Field, validator
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
from kiara.metadata import MetadataModel, ValueTypeAndDescription
from kiara.metadata.core_models import (
    ContextMetadataModel,
    DocumentationMetadataModel,
    OriginMetadataModel,
    PythonClassMetadata,
)
from kiara.module_config import ModuleTypeConfigSchema
from kiara.pipeline.config import PipelineConfig
from kiara.utils import create_table_from_field_schemas
from kiara.utils.output import create_table_from_base_model

if typing.TYPE_CHECKING:
    from kiara import KiaraModule


class KiaraModuleConfigMetadata(MetadataModel):
    @classmethod
    def from_config_class(
        cls,
        config_cls: typing.Type[ModuleTypeConfigSchema],
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


def calculate_class_doc_url(base_url: str, module_type_name: str, pipeline: bool):

    if base_url.endswith("/"):
        base_url = base_url[0:-1]

    module_type_name = module_type_name.replace(".", "")
    if not pipeline:
        url = f"{base_url}/latest/modules_list/#{module_type_name}"
    else:
        url = f"{base_url}/latest/pipelines_list/#{module_type_name}"

    return url


def calculate_class_source_url(
    base_url: str, python_class_info: PythonClassMetadata, branch: str = "main"
):

    if base_url.endswith("/"):
        base_url = base_url[0:-1]

    m = python_class_info.get_module()
    m_file = m.__file__

    base_url = f"{base_url}/blob/{branch}/src/{python_class_info.module_name.replace('.', '/')}"

    if m_file.endswith("__init__.py"):
        url = f"{base_url}/__init__.py"
    else:
        url = f"{base_url}.py"

    return url


class KiaraModuleTypeMetadata(MetadataModel):
    @classmethod
    def from_module_class(cls, module_cls: typing.Type["KiaraModule"]):

        module_attrs = cls.extract_module_attributes(module_cls=module_cls)
        return cls(**module_attrs)

    @classmethod
    def extract_module_attributes(
        self, module_cls: typing.Type["KiaraModule"]
    ) -> typing.Dict[str, typing.Any]:

        if not hasattr(module_cls, "process"):
            raise Exception(f"Module class '{module_cls}' misses 'process' method.")
        proc_src = textwrap.dedent(inspect.getsource(module_cls.process))  # type: ignore

        origin_md = OriginMetadataModel.from_class(module_cls)
        doc = DocumentationMetadataModel.from_class_doc(module_cls)
        python_class = PythonClassMetadata.from_class(module_cls)
        properties_md = ContextMetadataModel.from_class(module_cls)

        is_pipeline = module_cls.is_pipeline()
        doc_url = properties_md.get_url_for_reference("documentation")
        if doc_url:
            class_doc = calculate_class_doc_url(doc_url, module_cls._module_type_id, pipeline=is_pipeline)  # type: ignore
            properties_md.add_reference(
                "module_doc",
                class_doc,
                "A link to the published, auto-generated module documentation.",
            )

        if not is_pipeline:
            repo_url = properties_md.get_url_for_reference("source_repo")
            if repo_url is not None:
                src_url = calculate_class_source_url(repo_url, python_class)
                properties_md.add_reference(
                    "source_url",
                    src_url,
                    "A link to the published source file that contains this module.",
                )

        config = KiaraModuleConfigMetadata.from_config_class(module_cls._config_cls)
        pipeline_config = None
        if module_cls._module_type_id != "pipeline" and is_pipeline:  # type: ignore
            pipeline_config = module_cls._base_pipeline_config  # type: ignore

        return {
            "type_name": module_cls._module_type_name,  # type: ignore
            "type_id": module_cls._module_type_id,  # type: ignore
            "documentation": doc,
            "origin": origin_md,
            "context": properties_md,
            "python_class": python_class,
            "config": config,
            "is_pipeline": is_pipeline,
            "pipeline_config": pipeline_config,
            "process_src": proc_src,
        }

    type_name: str = Field(description="The registered name for this module type.")
    type_id: str = Field(description="The full type id.")
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
    pipeline_config: typing.Optional[PipelineConfig] = Field(
        description="If this module is a pipeline, this field contains the pipeline configuration.",
        default_factory=None,
    )
    process_src: str = Field(
        description="The source code of the process method of the module."
    )

    @validator("documentation", pre=True)
    def validate_doc(cls, value):

        return DocumentationMetadataModel.create(value)

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        include_config_schema = config.get("include_config_schema", True)
        include_src = config.get("include_src", True)
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

        if include_config_schema:
            if self.config:
                config_cls = self.config.python_class.get_class()
                table.add_row("Module config", create_table_from_base_model(config_cls))
            table.add_row("Module config", "-- no config --")

        table.add_row("Python class", self.python_class.create_renderable())

        if include_src:
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

        include_desc = config.get("include_doc", True)

        table = Table(box=box.SIMPLE, show_header=False, show_lines=True)
        table.add_column("Property", style="i")
        table.add_column("Value")

        if include_desc:
            table.add_row("Description", self.type_metadata.documentation.description)

        table.add_row("Origin", self.type_metadata.origin.create_renderable())
        table.add_row("Type context", self.type_metadata.context.create_renderable())
        table.add_row(
            "Python class", self.type_metadata.python_class.create_renderable()
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
        # table.add_row("Source code", self.type_metadata.process_src)

        return table
