# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import inspect
import orjson.orjson
import textwrap
from pydantic import Extra, PrivateAttr
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
from typing import TYPE_CHECKING, Any, Dict, Literal, Mapping, Optional, Type

from kiara.defaults import (
    DEFAULT_NO_DESC_VALUE,
    MODULE_CONFIG_METADATA_CATEGORY_ID,
    MODULE_CONFIG_SCHEMA_CATEGORY_ID,
)
from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.info import KiaraTypeInfoModel, TypeInfoModelGroup
from kiara.models.python_class import PythonClass
from kiara.models.values.value_schema import ValueSchema

if TYPE_CHECKING:
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
        default_factory=dict, description="Value constants for this module."
    )
    defaults: Dict[str, Any] = Field(
        default_factory=dict, description="Value defaults for this module."
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
    ):

        flat_models = get_flat_models_from_model(config_cls)
        model_name_map = get_model_name_map(flat_models)
        m_schema, _, _ = model_process_schema(config_cls, model_name_map=model_name_map)
        fields = m_schema["properties"]

        config_values = {}
        for field_name, details in fields.items():

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
        return self.python_class.model_id

    def _retrieve_category_id(self) -> str:
        return MODULE_CONFIG_METADATA_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.python_class.model_id


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

    base_url = f"{base_url}/blob/{branch}/src/{python_class_info.python_module_name.replace('.', '/')}"

    if m_file.endswith("__init__.py"):
        url = f"{base_url}/__init__.py"
    else:
        url = f"{base_url}.py"

    return url


class KiaraModuleTypeInfo(KiaraTypeInfoModel["KiaraModule"]):
    @classmethod
    def create_from_type_class(
        cls, type_cls: Type["KiaraModule"]
    ) -> "KiaraModuleTypeInfo":

        module_attrs = cls.extract_module_attributes(module_cls=type_cls)
        return cls.construct(**module_attrs)

    @classmethod
    def base_class(self) -> Type["KiaraModule"]:

        from kiara.modules import KiaraModule

        return KiaraModule

    @classmethod
    def category_name(cls) -> str:
        return "module"

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

    process_src: str = Field(
        description="The source code of the process method of the module."
    )

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
            config_cls = self.python_class.get_class()._config_cls  # type: ignore
            from kiara.utils.output import create_table_from_base_model_cls

            table.add_row(
                "Module config schema", create_table_from_base_model_cls(config_cls)
            )

        table.add_row("Python class", self.python_class.create_renderable())

        if include_src:
            _config = Syntax(self.process_src, "python", background_color="default")
            table.add_row("Processing source code", Panel(_config, box=box.HORIZONTALS))

        return table


class ModuleTypeClassesInfo(TypeInfoModelGroup):
    @classmethod
    def base_info_class(cls) -> Type[KiaraTypeInfoModel]:
        return KiaraModuleTypeInfo

    type_name: Literal["module_type"] = "module_type"
    type_infos: Mapping[str, KiaraModuleTypeInfo] = Field(
        description="The module type info instances for each type."
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
            "python_class_name": cls_name,
            "python_module_name": module_name,
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
