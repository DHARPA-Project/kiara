# -*- coding: utf-8 -*-
import inspect
import orjson.orjson
import textwrap
from pydantic import Extra, Field, PrivateAttr
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
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Optional, Type

from kiara.defaults import (
    DEFAULT_NO_DESC_VALUE,
    MODULE_CONFIG_METADATA_CATEGORY_ID,
    MODULE_CONFIG_SCHEMA_CATEGORY_ID,
    MODULE_TYPE_CATEGORY_ID,
    MODULE_TYPES_CATEGORY_ID,
)
from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.python_class import PythonClass
from kiara.models.values.value_schema import ValueSchema

if TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.modules import KiaraModule


class KiaraModuleConfig(KiaraModel):
    """Base class that describes the configuration a [``KiaraModule``][kiara.module.KiaraModule] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``KiaraModule`` class.

    There are two config options every ``KiaraModule`` supports:

     - ``constants``, and
     - ``defaults``

     Constants are pre-set inputs, and users can't change them and an error is thrown if they try. Defaults are default
     values that override the schema defaults, and those can be overwritten by users. If both a constant and a default
     value is set for an input field, an error is thrown.
    """

    @classmethod
    def requires_config(cls, config: Optional[Mapping[str, Any]] = None) -> bool:
        """Return whether this class can be used as-is, or requires configuration before an instance can be created."""

        for field_name, field in cls.__fields__.items():
            if field.required and field.default is None:
                if config:
                    if config.get(field_name, None) is None:
                        return True
                else:
                    return True
        return False

    _config_hash: str = PrivateAttr(default=None)
    constants: Dict[str, Any] = Field(
        default_factory=dict, description="ValueOrm constants for this module."
    )
    defaults: Dict[str, Any] = Field(
        default_factory=dict, description="ValueOrm defaults for this module."
    )

    class Config:
        extra = Extra.forbid
        validate_assignment = True

    def get(self, key: str) -> Any:
        """Get the value for the specified configuation key."""

        if key not in self.__fields__:
            raise Exception(
                f"No config value '{key}' in module config class '{self.__class__.__name__}'."
            )

        return getattr(self, key)

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return MODULE_CONFIG_SCHEMA_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:

        return self.dict()

    def create_renderable(self, **config: Any) -> RenderableType:

        my_table = Table(box=box.MINIMAL, show_header=False)
        my_table.add_column("Field name", style="i")
        my_table.add_column("Value")
        for field in self.__fields__:
            attr = getattr(self, field)
            if isinstance(attr, str):
                attr_str = attr
            elif hasattr(attr, "create_renderable"):
                attr_str = attr.create_renderable()
            elif isinstance(attr, BaseModel):
                attr_str = attr.json(option=orjson.orjson.OPT_INDENT_2)
            else:
                attr_str = str(attr)
            my_table.add_row(field, attr_str)

        return my_table

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False

        return self.model_data_hash == other.model_data_hash

    def __hash__(self):

        return self.model_data_hash


class ValueTypeAndDescription(BaseModel):

    description: str = Field(description="The description for the value.")
    type: str = Field(description="The value type.")
    value_default: Any = Field(description="Default for the value.", default=None)
    required: bool = Field(description="Whether this value is required")


class KiaraModuleConfigMetadata(KiaraModel):
    @classmethod
    def from_config_class(
        cls,
        config_cls: Type[KiaraModuleConfig],
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
        return self.python_class.id

    def _retrieve_category_id(self) -> str:
        return MODULE_CONFIG_METADATA_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.python_class.id


def calculate_class_doc_url(base_url: str, module_type_name: str):

    if base_url.endswith("/"):
        base_url = base_url[0:-1]

    module_type_name = module_type_name.replace(".", "")
    url = f"{base_url}/latest/modules_list/#{module_type_name}"

    return url


def calculate_class_source_url(
    base_url: str, python_class_info: PythonClass, branch: str = "main"
):

    if base_url.endswith("/"):
        base_url = base_url[0:-1]

    m = python_class_info.get_python_module()
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

        doc_url = properties_md.get_url_for_reference("documentation")
        if doc_url:
            class_doc = calculate_class_doc_url(doc_url, module_cls._module_type_name)  # type: ignore
            properties_md.add_reference(
                "module_doc",
                class_doc,
                "A link to the published, auto-generated module documentation.",
            )

        repo_url = properties_md.get_url_for_reference("source_repo")
        if repo_url is not None:
            src_url = calculate_class_source_url(repo_url, python_class)
            properties_md.add_reference(
                "source_url",
                src_url,
                "A link to the published source file that contains this module.",
            )

        config = KiaraModuleConfigMetadata.from_config_class(module_cls._config_cls)

        return {
            "type_name": module_cls._module_type_name,  # type: ignore
            "documentation": doc,
            "authors": authors_md,
            "context": properties_md,
            "python_class": python_class,
            "config": config,
            "process_src": proc_src,
        }

    type_name: str = Field(description="The registered name for this module type.")
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
    process_src: str = Field(
        description="The source code of the process method of the module."
    )

    def _retrieve_id(self) -> str:
        return self.type_name

    def _retrieve_category_id(self) -> str:
        return MODULE_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.type_name

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
                from kiara.utils.output import create_table_from_base_model_cls

                table.add_row(
                    "Module config", create_table_from_base_model_cls(config_cls)
                )
            else:
                table.add_row("Module config", "-- no config --")

        table.add_row("Python class", self.python_class.create_renderable())

        if include_src:
            _config = Syntax(self.process_src, "python", background_color="default")
            table.add_row("Processing source code", Panel(_config, box=box.HORIZONTALS))

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
            _cls = kiara.module_registry.get_module_class(tn)
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
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return MODULE_TYPES_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        obj = {k: v.model_data_hash for k, v in self.__root__.items()}
        return obj

    def create_renderable(self, **config: Any) -> RenderableType:

        return ModuleTypesGroupInfo.create_renderable_from_module_info_map(
            self.__root__
        )


class KiaraModuleClass(PythonClass):
    @classmethod
    def from_module(cls, module: "KiaraModule"):

        item_cls = module.__class__

        cls_name = item_cls.__name__
        module_name = item_cls.__module__
        if module_name == "builtins":
            full_name = cls_name
        else:
            full_name = f"{item_cls.__module__}.{item_cls.__name__}"

        conf: Dict[str, Any] = {
            "class_name": cls_name,
            "module_name": module_name,
            "full_name": full_name,
        }

        conf["module_config"] = module.config
        conf["inputs_schema"] = module.inputs_schema
        conf["outputs_schema"] = module.outputs_schema

        result = KiaraModuleClass.construct(**conf)
        result._cls_cache = item_cls
        result._module_instance_cache = module
        return result

    module_config: Dict[str, Any] = Field(description="The module config.")
    inputs_schema: Dict[str, ValueSchema] = Field(
        description="The schema for the module input(s)."
    )
    outputs_schema: Dict[str, ValueSchema] = Field(
        description="The schema for the module output(s)."
    )

    _module_instance_cache: "KiaraModule" = PrivateAttr(default=None)

    def get_kiara_module_instance(self) -> "KiaraModule":

        if self._module_instance_cache is not None:
            return self._module_instance_cache

        m_cls = self.get_class()
        self._module_instance_cache = m_cls(module_config=self.module_config)
        return self._module_instance_cache
