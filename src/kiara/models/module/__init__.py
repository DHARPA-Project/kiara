# -*- coding: utf-8 -*-
import inspect
import textwrap
from pydantic.class_validators import validator
from pydantic.fields import Field
from pydantic.main import BaseModel
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
from typing import Any, Dict, Iterable, Mapping, Optional, Type

from kiara.defaults import (
    DEFAULT_NO_DESC_VALUE,
    MODULE_CONFIG_METADATA_CATEGORY_ID,
    MODULE_TYPE_CATEGORY_ID,
    MODULE_TYPES_CATEGORY_ID,
)
from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.module.manifest import ModuleTypeConfigSchema
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.python_class import PythonClass
from kiara.utils.output import create_table_from_base_model_cls


class ValueTypeAndDescription(BaseModel):

    description: str = Field(description="The description for the value.")
    type: str = Field(description="The value type.")
    value_default: Any = Field(description="Default for the value.", default=None)
    required: bool = Field(description="Whether this value is required")


class KiaraModuleConfigMetadata(KiaraModel):
    @classmethod
    def from_config_class(
        cls,
        config_cls: Type[ModuleTypeConfigSchema],
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
            default = config_cls.__fields__[field_name].default
            if default is None:
                if callable(config_cls.__fields__[field_name].default_factory):
                    default = config_cls.__fields__[field_name].default_factory()  # type: ignore

            req = config_cls.__fields__[field_name].required

            config_values[field_name] = ValueTypeAndDescription(
                description=desc, type=type_str, value_default=default, required=req
            )

        python_cls = PythonClass.from_class(config_cls)
        return KiaraModuleConfigMetadata(
            python_class=python_cls, config_values=config_values
        )

    python_class: PythonClass = Field(description="The config model python class.")
    config_values: Dict[str, ValueTypeAndDescription] = Field(
        description="The available configuration values."
    )

    def _retrieve_id(self) -> str:
        self.python_class.id

    def _retrieve_category_id(self) -> str:
        return MODULE_CONFIG_METADATA_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.python_class.id


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
    base_url: str, python_class_info: PythonClass, branch: str = "main"
):

    if base_url.endswith("/"):
        base_url = base_url[0:-1]

    m = python_class_info.get_module()
    m_file = m.__file__
    assert m_file is not None

    base_url = f"{base_url}/blob/{branch}/src/{python_class_info.module_name.replace('.', '/')}"

    if m_file.endswith("__init__.py"):
        url = f"{base_url}/__init__.py"
    else:
        url = f"{base_url}.py"

    return url


class KiaraModuleTypeMetadata(KiaraModel):
    @classmethod
    def from_module_class(cls, module_cls: Type["KiaraModule"]):

        module_attrs = cls.extract_module_attributes(module_cls=module_cls)
        return cls(**module_attrs)

    @classmethod
    def extract_module_attributes(
        self, module_cls: Type["KiaraModule"]
    ) -> Dict[str, Any]:

        if not hasattr(module_cls, "process"):
            raise Exception(f"Module class '{module_cls}' misses 'process' method.")
        proc_src = textwrap.dedent(inspect.getsource(module_cls.process))  # type: ignore

        authors_md = AuthorsMetadataModel.from_class(module_cls)
        doc = DocumentationMetadataModel.from_class_doc(module_cls)
        python_class = PythonClass.from_class(module_cls)
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
            "authors": authors_md,
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
    authors: AuthorsMetadataModel = Field(
        description="Information about authorship for the module type."
    )
    context: ContextMetadataModel = Field(
        description="Generic properties of this module (description, tags, labels, references, ...)."
    )
    config: KiaraModuleConfigMetadata = Field(
        description="Details on how this module type can be configured."
    )
    python_class: PythonClass = Field(
        description="The python class that implements this module type."
    )
    is_pipeline: bool = Field(
        description="Whether the module type is a pipeline, or a core module."
    )
    pipeline_config: Optional[PipelineConfig] = Field(
        description="If this module is a pipeline, this field contains the pipeline configuration.",
        default_factory=None,
    )
    process_src: str = Field(
        description="The source code of the process method of the module."
    )

    def _retrieve_id(self) -> str:
        return self.type_id

    def _retrieve_category_id(self) -> str:
        return MODULE_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.type_id

    @validator("documentation", pre=True)
    def validate_doc(cls, value):

        return DocumentationMetadataModel.create(value)

    def create_renderable(self, **config: Any) -> RenderableType:

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
        table.add_row("Author(s)", self.authors.create_renderable())
        table.add_row("Context", self.context.create_renderable())

        if include_config_schema:
            if self.config:
                config_cls = self.config.python_class.get_class()
                table.add_row("Module config", create_table_from_base_model_cls(config_cls))
            else:
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


class ModuleTypesGroupInfo(KiaraModel):

    __root__: Dict[str, KiaraModuleTypeMetadata]

    # @classmethod
    # def from_type_names(
    #     cls,
    #     kiara: "Kiara",
    #     type_names: Optional[Iterable[str]] = None,
    #     ignore_pipeline_modules: bool = False,
    #     ignore_non_pipeline_modules: bool = False,
    #     **config: Any
    # ):
    #
    #     if ignore_pipeline_modules and ignore_non_pipeline_modules:
    #         raise Exception("Can't ignore both pipeline and non-pipeline modules.")
    #
    #     if type_names is None:
    #         type_names = kiara.available_module_types
    #
    #     classes = {}
    #     for tn in type_names:
    #         _cls = kiara.get_module_class(tn)
    #         if ignore_pipeline_modules and _cls.is_pipeline():
    #             continue
    #         if ignore_non_pipeline_modules and not _cls.is_pipeline():
    #             continue
    #         classes[tn] = KiaraModuleTypeMetadata.from_module_class(_cls)
    #
    #     return ModuleTypesGroupInfo(__root__=classes)

    @classmethod
    def create_renderable_from_type_names(
        cls,
        kiara: "Kiara",
        type_names: Iterable[str],
        ignore_pipeline_modules: bool = False,
        ignore_non_pipeline_modules: bool = False,
        **config: Any,
    ):

        classes = {}
        for tn in type_names:
            _cls = kiara.get_module_class(tn)
            classes[tn] = _cls

        return cls.create_renderable_from_module_type_map(
            module_types=classes,
            ignore_pipeline_modules=ignore_pipeline_modules,
            ignore_non_pipeline_modules=ignore_non_pipeline_modules,
            **config,
        )

    @classmethod
    def create_renderable_from_module_type_map(
        cls,
        module_types: Mapping[str, Type["KiaraModule"]],
        ignore_pipeline_modules: bool = False,
        ignore_non_pipeline_modules: bool = False,
        **config: Any,
    ):
        """Create a renderable from a map of module classes.

        Render-configuration options:
          - include_full_doc (default: False): include the full documentation, instead of just a one line description
        """

        return cls.create_renderable_from_module_info_map(
            {
                k: KiaraModuleTypeMetadata.from_module_class(v)
                for k, v in module_types.items()
            },
            ignore_pipeline_modules=ignore_pipeline_modules,
            ignore_non_pipeline_modules=ignore_non_pipeline_modules,
            **config,
        )

    @classmethod
    def create_renderable_from_module_info_map(
        cls,
        module_types: Mapping[str, KiaraModuleTypeMetadata],
        ignore_pipeline_modules: bool = False,
        ignore_non_pipeline_modules: bool = False,
        **config: Any,
    ):
        """Create a renderable from a map of module info wrappers.

        Render-configuration options:
          - include_full_doc (default: False): include the full documentation, instead of just a one line description
        """

        if ignore_pipeline_modules and ignore_non_pipeline_modules:
            raise Exception("Can't ignore both pipeline and non-pipeline modules.")

        if ignore_pipeline_modules:
            module_types = {k: v for k, v in module_types.items() if not v.is_pipeline}
        elif ignore_non_pipeline_modules:
            module_types = {k: v for k, v in module_types.items() if v.is_pipeline}

        show_lines = False
        table = Table(show_header=False, box=box.SIMPLE, show_lines=show_lines)
        table.add_column("name", style="b")
        table.add_column("desc", style="i")

        for name, details in module_types.items():

            if config.get("include_full_doc", False):
                table.add_row(name, details.documentation.full_doc)
            else:
                table.add_row(name, details.documentation.description)

        return table

    def _retrieve_id(self) -> str:
        return self.model_data_hash

    def _retrieve_category_id(self) -> str:
        return MODULE_TYPES_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        obj = {k: v.model_data_hash for k, v in self.__root__.items()}
        return obj

    def create_renderable(self, **config: Any) -> RenderableType:

        return ModuleTypesGroupInfo.create_renderable_from_module_info_map(
            self.__root__
        )
