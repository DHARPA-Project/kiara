# -*- coding: utf-8 -*-

import abc
import importlib
import inspect
import re
import textwrap
import uuid
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Mapping,
    Type,
    TypeVar,
    Union,
)

import humanfriendly
import orjson
from pydantic import BaseModel, Field, PrivateAttr, field_validator
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.tree import Tree

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorModel,
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.jobs import JobRecord
from kiara.models.module.operation import Operation
from kiara.models.module.pipeline import PipelineConfig, PipelineStep
from kiara.models.module.pipeline.structure import (
    PipelineStructure,
    StepInfo,
)
from kiara.models.module.pipeline.value_refs import PipelineInputRef, PipelineOutputRef
from kiara.models.python_class import PythonClass
from kiara.models.values import DataTypeCharacteristics, ValueStatus
from kiara.models.values.lineage import ValueLineage
from kiara.models.values.value import (
    ORPHAN,
    DataTypeInfo,
    PersistedData,
    Value,
    ValueMap,
    ValuePedigree,
)
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule
from kiara.renderers import KiaraRenderer
from kiara.utils import log_exception, log_message
from kiara.utils.class_loading import find_all_kiara_model_classes
from kiara.utils.cli import HORIZONTALS_NO_TO_AND_BOTTOM
from kiara.utils.dates import to_human_readable_date_string
from kiara.utils.json import orjson_dumps
from kiara.utils.output import extract_renderable

try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self  # type: ignore


if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.data_types import DataType
    from kiara.models.runtime_environment.python import PythonRuntimeEnvironment
    from kiara.models.values.value_metadata import ValueMetadata
    from kiara.operations import OperationType
    from kiara.registries.aliases import AliasRegistry
    from kiara.registries.data import DataRegistry

INFO_BASE_INSTANCE_TYPE = TypeVar("INFO_BASE_INSTANCE_TYPE")
INFO_BASE_CLASS = TypeVar("INFO_BASE_CLASS", bound=type)


def pretty_print_value_data_terminal(value: "ValueInfo"):
    assert value._value is not None
    assert value._value._data_registry is not None
    try:
        renderable = value._value._data_registry.pretty_print_data(
            value.value_id, target_type="terminal_renderable"
        )
    except Exception as e:
        log_exception(e)
        log_message("error.pretty_print", value=value.value_id, error=e)
        renderable = [str(value._value.data)]

    return renderable


RENDER_FIELDS: Dict[str, Dict[str, Any]] = {
    "value_id": {
        "show_default": True,
        "render": {"terminal": lambda v: str(v.value_id)},
    },
    "aliases": {
        "show_default": True,
        "render": {"terminal": lambda v: ", ".join(v.aliases)},
    },
    "type": {
        "show_default": True,
        "render": {"terminal": lambda x: x.value_schema.type},
    },
    "value_schema": {"show_default": False},
    "value_created": {
        "show_default": False,
        "render": {
            "terminal": lambda v: f"{to_human_readable_date_string(v.value_created)} ago"
        },
    },
    "is_persisted": {
        "show_default": False,
        "render": {"terminal": lambda v: "yes" if v.is_persisted else "no"},
    },
    "hash": {"show_default": False, "render": {"terminal": lambda v: v.value_hash}},
    "data": {
        "show_default": False,
        "render": {"terminal": pretty_print_value_data_terminal},
    },
    "pedigree": {
        "show_default": False,
        "render": {"terminal": lambda v: "-- external data -- " if v == ORPHAN else v},
    },
    "lineage": {"show_default": False},
    "load_config": {"show_default": False},
    "data_type_config": {
        "show_default": False,
        "render": {
            "terminal": lambda v: Syntax(
                orjson_dumps(v.value_schema.type_config, option=orjson.OPT_INDENT_2),
                "json",
                background_color="default",
            )
        },
    },
    "serialize_details": {
        "show_default": False,
        "render": {"terminal": lambda v: v.serialized.create_renderable()},
    },
    "properties": {
        "show_default": False,
        "render": {
            "terminal": lambda v: v.property_values.create_renderable(show_header=False)
        },
    },
    "size": {
        "show_default": True,
        "render": {"terminal": lambda v: humanfriendly.format_size(v.value_size)},
    },
}


class ValueTypeAndDescription(BaseModel):
    description: str = Field(description="The description for the value.")
    type: str = Field(description="The value type.")
    value_default: Any = Field(description="Default for the value.", default=None)
    required: bool = Field(description="Whether this value is required")


class ItemInfo(KiaraModel):
    """Base class that holds/manages information about an item within kiara."""

    @classmethod
    @abc.abstractmethod
    def base_instance_class(cls) -> Type:
        pass

    @classmethod
    @abc.abstractmethod
    def create_from_instance(cls, kiara: "Kiara", instance: Any, **kwargs):
        pass

    @field_validator("documentation", mode="before")
    @classmethod
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    type_name: str = Field(description="The registered name for this item type.")
    documentation: DocumentationMetadataModel = Field(
        description="Documentation for the item."
    )
    authors: AuthorsMetadataModel = Field(
        description="Information about authorship for the item."
    )
    context: ContextMetadataModel = Field(
        description="Generic properties of this item (description, tags, labels, references, ...)."
    )

    def _retrieve_id(self) -> str:
        return self.type_name

    def _retrieve_data_to_hash(self) -> Any:
        return self.type_name

    def create_renderable(self, **config: Any) -> RenderableType:
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

        if hasattr(self, "python_class"):
            table.add_row("Python class", self.python_class.create_renderable())  # type: ignore

        return table


class TypeInfo(ItemInfo, Generic[INFO_BASE_CLASS]):
    @classmethod
    def create_from_instance(cls, kiara: "Kiara", instance: INFO_BASE_CLASS, **kwargs):
        return cls.create_from_type_class(type_cls=instance, kiara=kiara)

    @classmethod
    @abc.abstractmethod
    def create_from_type_class(
        self, type_cls: INFO_BASE_CLASS, kiara: "Kiara"
    ) -> "TypeInfo":
        pass

    @classmethod
    def base_instance_class(self) -> INFO_BASE_CLASS:
        return type  # type: ignore

    python_class: PythonClass = Field(
        description="The python class that implements this module type."
    )


INFO_ITEM_TYPE = TypeVar("INFO_ITEM_TYPE", bound=ItemInfo)


class InfoItemGroup(KiaraModel, Generic[INFO_ITEM_TYPE]):
    @classmethod
    @abc.abstractmethod
    def base_info_class(cls) -> Type[INFO_ITEM_TYPE]:
        pass

    @classmethod
    def create_from_instances(
        cls,
        kiara: "Kiara",
        instances: Mapping[str, Any],
        **kwargs: Any,
    ) -> "InfoItemGroup[INFO_ITEM_TYPE]":
        info_cls = cls.base_info_class()
        items = {}
        for k in sorted(instances.keys()):
            v = instances[k]
            items[k] = info_cls.create_from_instance(kiara=kiara, instance=v, **kwargs)

        group_title = kwargs.pop("group_title", None)
        result = cls(group_title=group_title, item_infos=items)
        result._kiara = kiara
        return result

    group_title: Union[str, None] = Field(description="The group alias.", default=None)
    item_infos: Mapping[str, INFO_ITEM_TYPE] = Field(description="The info items.")
    _kiara: Union["Kiara", None] = PrivateAttr(default=None)

    def _retrieve_subcomponent_keys(self) -> Iterable[str]:
        return self.item_infos.keys()

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "type_name": self.__class__._kiara_model_name,  # type: ignore
            "included_types": list(self.item_infos.keys()),
        }  # type: ignore

    def create_renderable(self, **config: Any) -> RenderableType:
        full_doc = config.get("full_doc", False)

        table = Table(show_header=True, box=box.SIMPLE, show_lines=full_doc)
        table.add_column("Name", style="i")
        table.add_column("Description")

        for type_name in sorted(self.item_infos.keys()):
            t_md = self.item_infos[type_name]
            if full_doc:
                md = Markdown(t_md.documentation.full_doc)
            else:
                md = Markdown(t_md.documentation.description)
            table.add_row(type_name, md)

        return table

    def __getitem__(self, item: str) -> INFO_ITEM_TYPE:
        return self.item_infos[item]

    # def __iter__(self):
    #     return self.item_infos.__iter__()

    def __len__(self):
        return len(self.item_infos)


class TypeInfoItemGroup(InfoItemGroup[TypeInfo]):
    @classmethod
    @abc.abstractmethod
    def base_info_class(cls) -> Type[TypeInfo]:
        pass

    @classmethod
    def create_from_type_items(
        cls, kiara: "Kiara", group_title: Union[str, None] = None, **items: Type
    ) -> Self:
        type_infos: Mapping[str, TypeInfo[Any]] = {
            k: cls.base_info_class().create_from_type_class(type_cls=v, kiara=kiara)
            for k, v in items.items()
        }
        data_types_info = cls(group_title=group_title, item_infos=type_infos)
        return data_types_info


class KiaraModelTypeInfo(TypeInfo[Type[KiaraModel]]):
    _kiara_model_id: ClassVar = "info.kiara_model"

    @classmethod
    def create_from_type_class(
        self, type_cls: Type[KiaraModel], kiara: "Kiara"
    ) -> "KiaraModelTypeInfo":
        authors_md = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClass.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)
        type_name = type_cls._kiara_model_id  # type: ignore
        schema = type_cls.model_json_schema()

        return KiaraModelTypeInfo(
            type_name=type_name,
            documentation=doc,
            authors=authors_md,
            context=properties_md,
            python_class=python_class,
            metadata_schema=schema,
        )

    metadata_schema: Dict[str, Any] = Field(
        description="The (json) schema for this model data."
    )

    def create_renderable(self, **config: Any) -> RenderableType:
        include_doc = config.get("include_doc", True)
        include_schema = config.get("include_schema", False)

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

        if hasattr(self, "python_class"):
            table.add_row("Python class", self.python_class.create_renderable())

        if include_schema:
            schema = Syntax(
                orjson_dumps(self.metadata_schema, option=orjson.OPT_INDENT_2),
                "json",
                background_color="default",
            )
            table.add_row("metadata_schema", schema)

        return table


class KiaraModelClassesInfo(TypeInfoItemGroup):
    _kiara_model_id: ClassVar = "info.kiara_models"

    @classmethod
    def find_kiara_models(
        cls, alias: Union[str, None] = None, only_for_package: Union[str, None] = None
    ) -> "KiaraModelClassesInfo":
        models = find_all_kiara_model_classes()

        # we don't need the kiara instance, this is just to satisfy mypy
        kiara: Kiara = None  # type: ignore
        group: KiaraModelClassesInfo = KiaraModelClassesInfo.create_from_type_items(
            kiara=kiara, group_title=alias, **models
        )  # type: ignore

        if only_for_package:
            temp = {}
            for key, info in group.item_infos.items():
                if info.context.labels.get("package") == only_for_package:
                    temp[key] = info

            group = KiaraModelClassesInfo(
                group_id=group.instance_id,  # type: ignore
                group_title=group.group_title,
                item_infos=temp,  # type: ignore
            )

        return group

    @classmethod
    def base_info_class(cls) -> Type[KiaraModelTypeInfo]:
        return KiaraModelTypeInfo  # type: ignore

    type_name: Literal["kiara_model"] = "kiara_model"
    item_infos: Mapping[str, KiaraModelTypeInfo] = Field(  # type: ignore
        description="The value metadata info instances for each type."
    )


class ValueInfo(ItemInfo):
    _kiara_model_id: ClassVar = "info.value"

    @classmethod
    def base_instance_class(cls) -> Type[Value]:
        return Value

    @classmethod
    def create_from_instance(
        cls, kiara: "Kiara", instance: Value, **kwargs: Any
    ) -> "ValueInfo":
        resolve_aliases = kwargs.get("resolve_aliases", True)
        resolve_destinies = kwargs.get("resolve_destinies", True)
        resolve_properties = kwargs.get("resolve_properties", True)

        if resolve_aliases:
            aliases = sorted(
                kiara.alias_registry.find_aliases_for_value_id(instance.value_id)
            )
        else:
            aliases = None

        if instance.is_stored:
            persisted_details = kiara.data_registry.retrieve_persisted_value_details(
                value_id=instance.value_id
            )
        else:
            persisted_details = None

        if instance.data_type_name in kiara.type_registry.data_type_profiles:
            is_internal = "internal" in kiara.type_registry.get_type_lineage(
                instance.data_type_name
            )
        else:
            is_internal = False

        if resolve_destinies:
            destiny_links = (
                kiara.data_registry.retrieve_destinies_for_value_from_archives(
                    value_id=instance.value_id
                )
            )
            filtered_destinies = {}
            for alias, value_id in destiny_links.items():
                if (
                    alias in instance.property_links.keys()
                    and value_id == instance.property_links[alias]
                ):
                    continue
                filtered_destinies[alias] = value_id
        else:
            filtered_destinies = None

        if resolve_properties:
            properties = instance.get_all_property_data()
        else:
            properties = None

        authors = AuthorsMetadataModel()
        context = ContextMetadataModel()
        doc = DocumentationMetadataModel()

        model = ValueInfo(
            type_name=str(instance.value_id),
            documentation=doc,
            authors=authors,
            context=context,
            value_id=instance.value_id,
            kiara_id=instance.kiara_id,
            value_schema=instance.value_schema,
            value_created=instance.value_created,
            value_status=instance.value_status,
            environment_hashes=instance.environment_hashes,
            value_size=instance.value_size,
            value_hash=instance.value_hash,
            pedigree=instance.pedigree,
            pedigree_output_name=instance.pedigree_output_name,
            data_type_info=instance.data_type_info,
            # data_type_config=instance.data_type_config,
            # data_type_class=instance.data_type_class,
            property_links=instance.property_links,
            destiny_links=filtered_destinies,
            destiny_backlinks=instance.destiny_backlinks,
            aliases=aliases,
            serialized=persisted_details,
            properties=properties,
            is_internal=is_internal,
            is_persisted=instance._is_stored,
        )
        model._value = instance
        model._alias_registry = kiara.alias_registry  # type: ignore
        model._data_registry = instance._data_registry
        return model

    value_id: uuid.UUID = Field(description="The id of the value.")

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context this value belongs to."
    )

    value_schema: ValueSchema = Field(
        description="The schema that was used for this Value."
    )
    value_created: datetime = Field(description="The time this value was created.")
    value_status: ValueStatus = Field(description="The set/unset status of this value.")
    value_size: int = Field(description="The size of this value, in bytes.")
    value_hash: str = Field(description="The hash of this value.")
    pedigree: ValuePedigree = Field(
        description="Information about the module and inputs that went into creating this value."
    )
    pedigree_output_name: str = Field(
        description="The output name that produced this value (using the manifest inside the pedigree)."
    )
    data_type_info: DataTypeInfo = Field(
        description="Information about the underlying data type and it's configuration."
    )
    aliases: Union[List[str], None] = Field(
        description="The aliases that are registered for this value."
    )
    serialized: Union[PersistedData, None] = Field(
        description="Details for the serialization process that was used for this value."
    )
    properties: Union[Mapping[str, Any], None] = Field(
        description="Property data for this value.", default=None
    )
    destiny_links: Union[Mapping[str, uuid.UUID], None] = Field(
        description="References to all the values that act as destiny for this value in this context."
    )
    environment_hashes: Mapping[str, Mapping[str, str]] = Field(
        description="Hashes for the environments this value was created in."
    )
    enviroments: Union[Mapping[str, Mapping[str, Any]], None] = Field(
        description="Information about the environments this value was created in.",
        default=None,
    )
    property_links: Mapping[str, uuid.UUID] = Field(
        description="Links to values that are properties of this value.",
        default_factory=dict,
    )
    destiny_backlinks: Mapping[uuid.UUID, str] = Field(
        description="Backlinks to values that this value acts as destiny/or property for.",
        default_factory=dict,
    )
    is_internal: bool = Field(
        description="Whether this value is only used internally in kiara.",
        default=False,
    )
    is_persisted: bool = Field(
        description="Whether this value is stored in at least one data store."
    )
    _alias_registry: Union["AliasRegistry", None] = PrivateAttr(default=None)
    _data_registry: Union["DataRegistry", None] = PrivateAttr(default=None)
    _value: Union[Value, None] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(self.value_id)

    def _retrieve_data_to_hash(self) -> Any:
        return self.value_id.bytes

    @property
    def property_values(self) -> "ValueMap":
        assert self._value is not None
        return self._value.property_values

    @property
    def lineage(self) -> "ValueLineage":
        assert self._value is not None
        return self._value.lineage

    def resolve_aliases(self):
        if self.aliases is None:
            aliases = self._alias_registry.find_aliases_for_value_id(self.value_id)
            if aliases:
                aliases = sorted(aliases)
            self.aliases = aliases

    def resolve_destinies(self):
        if self.destiny_links is None:
            destiny_links = (
                self._value._data_registry.retrieve_destinies_for_value_from_archives(
                    value_id=self.value_id
                )
            )
            filtered_destinies = {}
            for alias, value_id in destiny_links.items():
                if (
                    alias in self.property_links.keys()
                    and value_id == self.property_links[alias]
                ):
                    continue
                filtered_destinies[alias] = value_id
            self.destiny_links = filtered_destinies

    def create_info_data(self, **config: Any) -> Mapping[str, Any]:
        assert self._value is not None
        return self._value.create_info_data(**config)

    def create_renderable(self, **render_config: Any) -> RenderableType:
        assert self._value is not None
        return self._value.create_renderable(**render_config)


class ValuesInfo(InfoItemGroup[ValueInfo]):
    @classmethod
    def base_info_class(cls) -> Type[ValueInfo]:
        return ValueInfo

    def create_render_map(
        self, render_type: str, default_render_func: Callable, **render_config
    ):
        list_by_alias = render_config.get("list_by_alias", True)
        show_internal = render_config.get("show_internal_values", False)

        render_fields = render_config.get("render_fields", None)
        if not render_fields:
            render_fields = [k for k, v in RENDER_FIELDS.items() if v["show_default"]]
            if list_by_alias:
                render_fields[0] = "aliases"
                render_fields[1] = "value_id"

        render_map: Dict[uuid.UUID, Dict[str, Any]] = {}

        lookup = {}
        value_info: ValueInfo
        for value_info in self.item_infos.values():  # type: ignore
            if not show_internal and value_info.is_internal:
                continue
            lookup[value_info.value_id] = value_info

            details = {}
            for property in render_fields:
                render_func = (
                    RENDER_FIELDS.get(property, {})
                    .get("render", {})
                    .get(render_type, None)
                )
                if render_func is None:
                    if hasattr(value_info, property):
                        attr = getattr(value_info, property)
                        rendered = default_render_func(attr)
                    else:
                        raise Exception(
                            f"Can't render property '{property}': no render function registered and not a property."
                        )
                else:
                    rendered = render_func(value_info)
                details[property] = rendered
            render_map[value_info.value_id] = details

        if not list_by_alias:
            return {str(k): v for k, v in render_map.items()}
        else:
            result: Dict[str, Dict[str, Any]] = {}
            for value_id, render_details in render_map.items():
                value_aliases = lookup[value_id].aliases
                if value_aliases:
                    for alias in value_aliases:
                        assert alias not in result.keys()
                        render_details = dict(render_details)
                        render_details["alias"] = alias
                        result[alias] = render_details
                else:
                    render_details["alias"] = ""
                    result[f"no_aliases_{value_id}"] = render_details
            return result

    def create_renderable(self, render_type: str = "terminal", **render_config: Any):
        render_map = self.create_render_map(
            render_type=render_type,
            default_render_func=extract_renderable,
            **render_config,
        )

        list_by_alias = render_config.get("list_by_alias", True)
        render_fields = render_config.get("render_fields", None)
        if not render_fields:
            render_fields = [k for k, v in RENDER_FIELDS.items() if v["show_default"]]
        if list_by_alias:
            render_fields.insert(0, "alias")
            render_fields.remove("aliases")

        table = Table(box=box.SIMPLE)
        for property in render_fields:
            if property == "aliases" and list_by_alias:
                table.add_column("alias")
            elif property == "size":
                table.add_column("size", justify="right")
            else:
                table.add_column(property)

        for item_id, details in render_map.items():
            row = []
            for field in render_fields:
                value = details[field]
                row.append(value)
            table.add_row(*row)

        return table


class KiaraModuleConfigMetadata(KiaraModel):
    _kiara_model_id: ClassVar = "metadata.module_config"

    @classmethod
    def from_config_class(
        cls,
        config_cls: Type[KiaraModuleConfig],
    ):
        schema = config_cls.model_json_schema()
        fields = schema["properties"]

        config_values = {}
        for field_name, details in fields.items():
            type_str = "unknown"
            if "type" in details.keys():
                type_str = details["type"]
            elif "anyOf" in details.keys():
                type_str = f"anyOf: {details['anyOf']}"
            elif "allOf" in details.keys():
                type_str = f"allOf: {details['allOf']}"

            desc = details.get("description", DEFAULT_NO_DESC_VALUE)
            default = config_cls.model_fields[field_name].default
            if default is None:
                if callable(config_cls.model_fields[field_name].default_factory):
                    default = config_cls.model_fields[field_name].default_factory()  # type: ignore

            req = config_cls.model_fields[field_name].is_required()

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
        return self.python_class.full_name

    def _retrieve_data_to_hash(self) -> Any:
        return self.python_class.full_name


class MetadataTypeInfo(TypeInfo):
    _kiara_model_id: ClassVar = "info.metadata_type"

    @classmethod
    def create_from_type_class(
        self, type_cls: Type["ValueMetadata"], kiara: "Kiara"
    ) -> "MetadataTypeInfo":
        authors_md = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClass.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)
        type_name = type_cls._metadata_key  # type: ignore
        schema = type_cls.model_json_schema()

        return MetadataTypeInfo(
            type_name=type_name,
            documentation=doc,
            authors=authors_md,
            context=properties_md,
            python_class=python_class,
            metadata_schema=schema,
        )

    @classmethod
    def base_class(self) -> Type["ValueMetadata"]:
        from kiara.models.values.value_metadata import ValueMetadata

        return ValueMetadata

    @classmethod
    def category_name(cls) -> str:
        return "value_metadata"

    metadata_schema: Dict[str, Any] = Field(
        description="The (json) schema for this metadata value."
    )

    def create_renderable(self, **config: Any) -> RenderableType:
        include_doc = config.get("include_doc", True)
        include_schema = config.get("include_schema", True)

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

        if hasattr(self, "python_class"):
            table.add_row("Python class", self.python_class.create_renderable())

        if include_schema:
            schema = Syntax(
                orjson_dumps(self.metadata_schema, option=orjson.OPT_INDENT_2),
                "json",
                background_color="default",
            )
            table.add_row("metadata_schema", schema)

        return table


class MetadataTypeClassesInfo(TypeInfoItemGroup):
    _kiara_model_id: ClassVar = "info.metadata_types"

    @classmethod
    def base_info_class(cls) -> Type[TypeInfo]:
        return MetadataTypeInfo

    type_name: Literal["value_metadata"] = "value_metadata"
    item_infos: Mapping[str, MetadataTypeInfo] = Field(  # type: ignore
        description="The value metadata info instances for each type."
    )


class DataTypeClassInfo(TypeInfo[Type["DataType"]]):
    _kiara_model_id: ClassVar = "info.data_type"

    @classmethod
    def create_from_type_class(
        self, type_cls: Type["DataType"], kiara: Union["Kiara", None] = None
    ) -> "DataTypeClassInfo":
        from kiara.utils.metadata import get_metadata_model_for_data_type

        authors = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)

        if kiara is None:
            raise NotImplementedError(
                "Kiara instance is required to create DataTypeClassInfo."
            )
        else:
            data_type_name = getattr(type_cls, "_data_type_name", None)
            if not data_type_name:
                raise KiaraException(
                    f"Data type class '{type_cls.__name__}' does not have a '_data_type_name' attribute."
                )
            metadata_models = get_metadata_model_for_data_type(
                kiara=kiara, data_type=data_type_name
            )

        if kiara is not None:
            qual_profiles = kiara.type_registry.get_associated_profiles(
                type_cls._data_type_name  # type: ignore
            )  # type: ignore
            lineage = kiara.type_registry.get_type_lineage(type_cls._data_type_name)  # type: ignore
        else:
            qual_profiles = None
            lineage = None

        try:
            result = DataTypeClassInfo(
                type_name=type_cls._data_type_name,  # type: ignore
                python_class=PythonClass.from_class(type_cls),
                value_cls=PythonClass.from_class(type_cls.python_class()),
                data_type_config_cls=PythonClass.from_class(
                    type_cls.data_type_config_class()
                ),
                lineage=lineage,  # type: ignore
                qualifier_profiles=qual_profiles,
                documentation=doc,
                authors=authors,
                context=properties_md,
                supported_properties=metadata_models,
            )
        except Exception as e:
            if isinstance(
                e, TypeError
            ) and "missing 1 required positional argument: 'cls'" in str(e):
                raise Exception(
                    f"Invalid implementation of TypeValue subclass '{type_cls.__name__}': 'python_class' method must be marked as a '@classmethod'. This is a bug."
                )
            raise e

        result._kiara = kiara
        return result

    @classmethod
    def base_class(self) -> Type["DataType"]:
        from kiara.data_types import DataType

        return DataType

    @classmethod
    def category_name(cls) -> str:
        return "data_type"

    value_cls: PythonClass = Field(description="The python class of the value itself.")
    data_type_config_cls: PythonClass = Field(
        description="The python class holding the schema for configuring this type."
    )
    lineage: Union[List[str], None] = Field(description="This types lineage.")
    qualifier_profiles: Union[Mapping[str, Mapping[str, Any]], None] = Field(
        description="A map of qualifier profiles for this data types."
    )
    supported_properties: MetadataTypeClassesInfo = Field(
        description="The supported property types for this data type."
    )
    _kiara: Union["Kiara", None] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return self.type_name

    def _retrieve_data_to_hash(self) -> Any:
        return self.type_name

    def create_renderable(self, **config: Any) -> RenderableType:
        include_doc = config.get("include_doc", True)
        include_lineage = config.get("include_lineage", True)
        include_qualifer_profiles = config.get("include_qualifier_profiles", True)

        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
        table.add_column("property", style="i")
        table.add_column("value")

        if include_lineage:
            if self.lineage:
                table.add_row("lineage", "\n".join(self.lineage[0:]))
            else:
                table.add_row("lineage", "-- n/a --")

        if include_qualifer_profiles:
            if self.qualifier_profiles:
                qual_table = Table(show_header=False, box=box.SIMPLE)
                qual_table.add_column("name")
                qual_table.add_column("config")
                for name, details in self.qualifier_profiles.items():
                    json_details = orjson_dumps(details, option=orjson.OPT_INDENT_2)
                    qual_table.add_row(
                        name, Syntax(json_details, "json", background_color="default")
                    )
                table.add_row("qualifier profile(s)", qual_table)
            else:
                table.add_row("qualifier profile(s)", "-- n/a --")

        if include_doc:
            table.add_row(
                "Documentation",
                Panel(self.documentation.create_renderable(), box=box.SIMPLE),
            )

        table.add_row("Author(s)", self.authors.create_renderable())
        table.add_row("Context", self.context.create_renderable())

        table.add_row("Python class", self.python_class.create_renderable())
        table.add_row("Config class", self.data_type_config_cls.create_renderable())
        table.add_row("Value class", self.value_cls.create_renderable())

        return table


class DataTypeClassesInfo(TypeInfoItemGroup):
    _kiara_model_id: ClassVar = "info.data_types"

    # @classmethod
    # def create_from_type_items(
    #     cls,
    #     group_title: Union[str, None] = None,
    #     **items: Type,
    # ) -> "TypeInfoModelGroup":
    #
    #     type_infos = {
    #         k: cls.base_info_class().create_from_type_class(v) for k, v in items.items()  # type: ignore
    #     }
    #     data_types_info = cls(group_alias=group_title, item_infos=type_infos)  # type: ignore
    #     return data_types_info
    #
    # @classmethod
    # def create_augmented_from_type_items(
    #     cls,
    #     kiara: Union["Kiara", None] = None,
    #     group_alias: Union[str, None] = None,
    #     **items: Type,
    # ) -> "TypeInfoModelGroup":
    #
    #     type_infos = {
    #         k: cls.base_info_class().create_from_type_class(v, kiara=kiara) for k, v in items.items()  # type: ignore
    #     }
    #     data_types_info = cls(group_alias=group_alias, item_infos=type_infos)  # type: ignore
    #     data_types_info._kiara = kiara
    #     return data_types_info

    @classmethod
    def base_info_class(cls) -> Type[DataTypeClassInfo]:
        return DataTypeClassInfo

    type_name: Literal["data_type"] = "data_type"
    item_infos: Mapping[str, DataTypeClassInfo] = Field(  # type: ignore
        description="The data_type info instances for each type."
    )
    # _kiara: Union["Kiara", None] = PrivateAttr(default=None)

    def create_renderable(self, **config: Any) -> RenderableType:
        full_doc = config.get("full_doc", False)
        show_subtypes_inline = config.get("show_qualifier_profiles_inline", False)
        show_lineage = config.get("show_type_lineage", True)

        show_lines = full_doc or show_subtypes_inline or show_lineage

        table = Table(show_header=True, box=box.SIMPLE, show_lines=show_lines)
        table.add_column("type name", style="i")

        if show_lineage:
            table.add_column("type lineage")

        if show_subtypes_inline:
            table.add_column("(qualifier) profiles")

        if full_doc:
            table.add_column("documentation")
        else:
            table.add_column("description")

        all_types = self.item_infos.keys()

        for type_name in sorted(all_types):  # type: ignore
            t_md = self.item_infos[type_name]  # type: ignore
            row: List[Any] = [type_name]
            if show_lineage:
                lineage = t_md.lineage
                if lineage is None:
                    lineage_str = "-- n/a --"
                else:
                    lineage_str = ", ".join(reversed(lineage[1:]))
                row.append(lineage_str)
            if show_subtypes_inline:
                qual_profiles = t_md.qualifier_profiles
                if not qual_profiles:
                    qual_profiles_str = "-- n/a --"
                else:
                    qual_profiles_str = "\n".join(qual_profiles)
                row.append(qual_profiles_str)

            if full_doc:
                md = Markdown(t_md.documentation.full_doc)
            else:
                md = Markdown(t_md.documentation.description)
            row.append(md)
            table.add_row(*row)

        return table


class ModuleTypeInfo(TypeInfo[Type["KiaraModule"]]):
    _kiara_model_id: ClassVar = "info.kiara_module_type"

    @classmethod
    def create_from_type_class(
        cls, type_cls: Type["KiaraModule"], kiara: "Kiara"
    ) -> "ModuleTypeInfo":  # type: ignore
        module_attrs = cls.extract_module_attributes(module_cls=type_cls)
        return cls(**module_attrs)

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

        module_src = textwrap.dedent(inspect.getsource(module_cls))  # type: ignore

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
            "module_config": config,
            "module_src": module_src,
        }

    module_config: KiaraModuleConfigMetadata = Field(
        description="The module config metadata."
    )
    module_src: str = Field(
        description="The source code of the process method of the module."
    )

    def create_renderable(self, **config: Any) -> RenderableType:
        include_config_schema = config.get("include_config_schema", True)
        include_src = config.get("include_src", False)
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
            from kiara.context.config import KIARA_SETTINGS

            _config = Syntax(
                self.module_src,
                "python",
                background_color=KIARA_SETTINGS.syntax_highlight_background,
            )
            table.add_row("Processing source code", Panel(_config, box=box.HORIZONTALS))

        return table


class ModuleTypesInfo(TypeInfoItemGroup):
    _kiara_model_id: ClassVar = "info.module_types"

    @classmethod
    def base_info_class(cls) -> Type[TypeInfo]:
        return ModuleTypeInfo

    type_name: Literal["module_type"] = "module_type"
    item_infos: Mapping[str, ModuleTypeInfo] = Field(  # type: ignore
        description="The module type info instances for each type."
    )


class OperationTypeInfo(TypeInfo[Type["OperationType"]]):
    _kiara_model_id: ClassVar = "info.operation_type"

    @classmethod
    def create_from_type_class(  # type: ignore
        cls,
        kiara: "Kiara",
        type_cls: Type["OperationType"],  # type: ignore
    ) -> "OperationTypeInfo":
        authors_md = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClass.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)

        return OperationTypeInfo(
            type_name=type_cls._operation_type_name,  # type: ignore
            documentation=doc,
            authors=authors_md,
            context=properties_md,
            python_class=python_class,
        )

    @classmethod
    def base_class(self) -> Type["OperationType"]:
        from kiara.operations import OperationType

        return OperationType

    @classmethod
    def category_name(cls) -> str:
        return "operation_type"

    def _retrieve_id(self) -> str:
        return self.type_name

    def _retrieve_data_to_hash(self) -> Any:
        return self.type_name


class OperationTypeClassesInfo(TypeInfoItemGroup):
    _kiara_model_id: ClassVar = "info.operation_types"

    @classmethod
    def base_info_class(cls) -> Type[OperationTypeInfo]:  # type: ignore
        return OperationTypeInfo

    type_name: Literal["operation_type"] = "operation_type"
    item_infos: Mapping[str, OperationTypeInfo] = Field(  # type: ignore
        description="The operation info instances for each type."
    )


class FieldInfo(BaseModel):
    field_name: str = Field(description="The field name.")
    field_schema: ValueSchema = Field(description="The schema of the field.")
    data_type_info: DataTypeInfo = Field(
        description="Information about the data type instance of the associated value."
    )
    value_required: bool = Field(
        description="Whether user input is required (meaning: 'optional' is False, and no default set)."
    )


class PipelineStructureInfo(ItemInfo):
    _kiara_model_id: ClassVar = "info.pipeline_structure"

    @classmethod
    def base_instance_class(cls) -> Type[PipelineStructure]:
        return PipelineStructure

    @classmethod
    def create_from_instance(
        cls, kiara: "Kiara", instance: PipelineStructure, **kwargs
    ):
        authors = AuthorsMetadataModel()
        context = ContextMetadataModel()

        execution_graph: Dict[str, Any] = {}
        data_flow_graph: Dict[str, Any] = {}
        data_flow_graph_simple: Dict[str, Any] = {}

        input_fields = {}
        for field_name, schema in instance.pipeline_inputs_schema.items():
            dt = kiara.type_registry.get_data_type_instance(
                type_name=schema.type, type_config=schema.type_config
            )
            dt_info = FieldInfo(
                field_name=field_name,
                field_schema=schema,
                data_type_info=dt.info,
                value_required=schema.is_required(),
            )
            input_fields[field_name] = dt_info

        output_fields = {}
        for field_name, schema in instance.pipeline_outputs_schema.items():
            dt = kiara.type_registry.get_data_type_instance(
                type_name=schema.type, type_config=schema.type_config
            )
            dt_info = FieldInfo(
                field_name=field_name,
                field_schema=schema,
                data_type_info=dt.info,
                value_required=schema.is_required(),
            )
            output_fields[field_name] = dt_info

        return cls(
            type_name=instance.instance_id,
            documentation=instance.pipeline_config.doc,
            authors=authors,
            context=context,
            pipeline_config=instance.pipeline_config,
            pipeline_config_orig=instance.pipeline_config.get_raw_config(),
            # steps={step.step_id: step for step in instance.steps},
            step_details=instance.steps_details,
            input_aliases=instance.input_aliases,
            output_aliases=instance.output_aliases,
            constants=instance.constants,
            defaults=instance.defaults,
            pipeline_input_fields=input_fields,
            pipeline_output_fields=output_fields,
            pipeline_input_refs=instance.pipeline_input_refs,
            pipeline_output_refs=instance.pipeline_output_refs,
            execution_graph=execution_graph,
            data_flow_graph=data_flow_graph,
            data_flow_graph_simple=data_flow_graph_simple,
            processing_stages=instance.processing_stages,
            # processing_stages_info=instance.processing_stages_info,
        )

    pipeline_config: PipelineConfig = Field(
        description="The underlying pipeline config."
    )
    pipeline_config_orig: Dict[str, Any] = Field(
        description="The original, user-provided pipeline config."
    )
    # steps: Mapping[str, PipelineStep] = Field(
    #     description="All steps for this pipeline, indexed by their step_id.", exclude=True
    # )
    step_details: Mapping[str, StepInfo] = Field(
        description="Additional information for each step."
    )
    input_aliases: Dict[str, str] = Field(description="The input aliases.")
    output_aliases: Dict[str, str] = Field(description="The output aliases.")
    constants: Mapping[str, Any] = Field(
        description="The input constants for this pipeline."
    )
    defaults: Mapping[str, Any] = Field(
        description="The default inputs for this pipeline."
    )

    pipeline_input_fields: Mapping[str, FieldInfo] = Field(
        description="The pipeline inputs schema."
    )
    pipeline_output_fields: Mapping[str, FieldInfo] = Field(
        description="The pipeline outputs schema."
    )

    pipeline_input_refs: Mapping[str, PipelineInputRef] = Field(
        description="References to the step inputs that are linked to pipeline inputs."
    )
    pipeline_output_refs: Mapping[str, PipelineOutputRef] = Field(
        description="References to the step outputs that are linked to pipeline outputs."
    )

    execution_graph: Dict[str, Any] = Field(
        description="Data describing the execution graph of this pipeline."
    )
    data_flow_graph: Dict[str, Any] = Field(
        description="Data describing the data flow of this pipeline."
    )
    data_flow_graph_simple: Dict[str, Any] = Field(
        description="Data describing the (simplified) data flow of this pipeline."
    )

    processing_stages: List[List[str]] = Field(
        description="A list of lists, containing all the step_ids per stage, in the order of execution."
    )
    # processing_stages_info: Mapping[int, PipelineStage] = Field(
    #     description="More detailed information about each step of this pipelines execution graph."
    # )

    def get_step(self, step_id) -> PipelineStep:
        return self.step_details[step_id].step

    def get_step_details(self, step_id: str) -> StepInfo:
        return self.step_details[step_id]

    def create_renderable(self, **config: Any) -> RenderableType:
        tree = Tree("pipeline")
        inputs = tree.add("inputs")
        for field_name, field_info in self.pipeline_input_fields.items():
            inputs.add(f"[i]{field_name}[i] (type: {field_info.field_schema.type})")

        steps = tree.add("steps")
        for idx, stage in enumerate(self.processing_stages, start=1):
            stage_node = steps.add(f"stage {idx}")
            for step_id in stage:
                step_node = stage_node.add(f"step: {step_id}")
                step = self.get_step(step_id=step_id)
                if step.doc.is_set:
                    step_node.add(f"desc: {step.doc.description}")
                step_node.add(f"operation: {step.manifest_src.module_type}")

        outputs = tree.add("outputs")
        for field_name, field_info in self.pipeline_output_fields.items():
            outputs.add(f"[i]{field_name}[i] (type: {field_info.field_schema.type})")

        return tree


class OperationInfo(ItemInfo):
    _kiara_model_id: ClassVar = "info.operation"

    @classmethod
    def base_instance_class(cls) -> Type[Operation]:
        return Operation

    @classmethod
    def create_from_instance(cls, kiara: "Kiara", instance: Operation, **kwargs):
        return cls.create_from_operation(kiara=kiara, operation=instance)

    @classmethod
    def create_from_operation(
        cls, kiara: "Kiara", operation: Operation
    ) -> "OperationInfo":
        module = operation.module
        module_cls = module.__class__

        authors_md = AuthorsMetadataModel.from_class(module_cls)
        properties_md = ContextMetadataModel.from_class(module_cls)

        op_types = kiara.operation_registry.find_all_operation_types(
            operation_id=operation.operation_id
        )

        input_fields = {}
        for field_name, schema in operation.inputs_schema.items():
            try:
                dt = kiara.type_registry.get_data_type_instance(
                    type_name=schema.type, type_config=schema.type_config
                )
                dt_info = FieldInfo(
                    field_name=field_name,
                    field_schema=schema,
                    data_type_info=dt.info,
                    value_required=schema.is_required(),
                )
            except Exception:
                dtc = PythonClass.from_class(object)
                dti = DataTypeInfo(
                    data_type_name=f"{schema.type} (invalid)",
                    data_type_config=schema.type_config,
                    characteristics=DataTypeCharacteristics(),
                    data_type_class=dtc,
                )
                dt_info = FieldInfo(
                    field_name=field_name,
                    field_schema=schema,
                    data_type_info=dti,
                    value_required=schema.is_required(),
                )

            input_fields[field_name] = dt_info

        output_fields = {}
        for field_name, schema in operation.outputs_schema.items():
            dt = kiara.type_registry.get_data_type_instance(
                type_name=schema.type, type_config=schema.type_config
            )
            dt_info = FieldInfo(
                field_name=field_name,
                field_schema=schema,
                data_type_info=dt.info,
                value_required=schema.is_required(),
            )
            output_fields[field_name] = dt_info

        op_info = OperationInfo(
            type_name=operation.operation_id,
            operation_types=list(op_types),
            input_fields=input_fields,
            output_fields=output_fields,
            operation=operation,
            documentation=operation.doc,
            authors=authors_md,
            context=properties_md,
        )

        return op_info

    @classmethod
    def category_name(cls) -> str:
        return "operation"

    operation: Operation = Field(description="The operation instance.")
    operation_types: List[str] = Field(
        description="The operation types this operation belongs to."
    )
    input_fields: Mapping[str, FieldInfo] = Field(
        description="The inputs schema for this operation."
    )
    output_fields: Mapping[str, FieldInfo] = Field(
        description="The outputs schema for this operation."
    )

    def create_renderable(self, **config: Any) -> RenderableType:
        include_doc = config.get("include_doc", False)
        include_module_details = config.get("include_module_details", True)
        include_op_details = config.get("include_op_details", True)

        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
        table.add_column("property", style="i")
        table.add_column("value")

        if include_doc:
            table.add_row(
                "Documentation",
                Panel(self.documentation.create_renderable(), box=box.SIMPLE),
            )
        table.add_row("Author(s)", self.authors.create_renderable(**config))
        table.add_row("Context", self.context.create_renderable(**config))

        if include_module_details:
            table.add_row("Module type", self.operation.module_type)
            if self.operation.module_config:
                table.add_row(
                    "Module config",
                    Syntax(
                        orjson_dumps(
                            self.operation.module_config, option=orjson.OPT_INDENT_2
                        ),
                        "json",
                        background_color="default",
                    ),
                )

        if include_op_details:
            table.add_row(
                "Operation details", self.operation.create_renderable(**config)
            )
        return table


class OperationGroupInfo(InfoItemGroup):
    _kiara_model_id: ClassVar = "info.operations"

    @classmethod
    def base_info_class(cls) -> Type[ItemInfo]:
        return OperationInfo

    @classmethod
    def create_from_operations(
        cls, kiara: "Kiara", group_title: Union[str, None] = None, **items: Operation
    ) -> "OperationGroupInfo":
        op_infos = {
            k: OperationInfo.create_from_operation(kiara=kiara, operation=v)
            for k, v in items.items()
        }

        op_group_info = cls(group_title=group_title, item_infos=op_infos)
        return op_group_info

    # type_name: Literal["operation_type"] = "operation_type"
    item_infos: Mapping[str, OperationInfo] = Field(
        description="The operation info instances for each type."
    )

    def create_renderable(self, **config: Any) -> RenderableType:
        by_type = config.get("by_type", False)

        if by_type:
            return self._create_renderable_by_type(**config)
        else:
            return self._create_renderable_list(**config)

    def _create_renderable_list(self, **config) -> RenderableType:
        include_internal_operations = config.get("include_internal_operations", True)
        full_doc = config.get("full_doc", False)
        filter = config.get("filter", [])
        show_internal_column = config.get("show_internal_column", False)

        table = Table(box=box.SIMPLE, show_header=True)
        table.add_column("Id", no_wrap=True, style="i")
        table.add_column("Type(s)", style="green")
        if show_internal_column:
            table.add_column("Internal", justify="center")
        table.add_column("Description")

        for op_id, op_info in self.item_infos.items():
            if (
                not include_internal_operations
                and op_info.operation.operation_details.is_internal_operation
            ):
                continue

            types = op_info.operation_types

            if "custom_module" in types:
                types.remove("custom_module")

            desc_str = op_info.documentation.description
            if full_doc:
                desc = Markdown(op_info.documentation.full_doc)
            else:
                desc = Markdown(op_info.documentation.description)

            is_internal = op_info.operation.module.characteristics.is_internal
            is_internal_str = "\u2714" if is_internal else ""

            if filter:
                match = True
                for f in filter:
                    if (
                        f.lower() not in op_id.lower()
                        and f.lower() not in desc_str.lower()
                    ):
                        match = False
                        break
                if match:
                    if not show_internal_column:
                        table.add_row(op_id, ", ".join(types), desc)
                    else:
                        table.add_row(op_id, ", ".join(types), is_internal_str, desc)

            else:
                if not show_internal_column:
                    table.add_row(op_id, ", ".join(types), desc)
                else:
                    table.add_row(op_id, ", ".join(types), is_internal_str, desc)

        return table

    def _create_renderable_by_type(self, **config) -> Table:
        include_internal_operations = config.get("include_internal_operations", True)
        full_doc = config.get("full_doc", False)
        filter = config.get("filter", [])

        by_type: Dict[str, Dict[str, OperationInfo]] = {}
        for op_id, op in self.item_infos.items():
            if filter:
                match = True
                for f in filter:
                    if (
                        f.lower() not in op_id.lower()
                        and f.lower() not in op.documentation.description.lower()
                    ):
                        match = False
                        break
                if not match:
                    continue
            for op_type in op.operation_types:
                by_type.setdefault(op_type, {})[op_id] = op

        table = Table(box=box.SIMPLE, show_header=True)
        table.add_column("Type", no_wrap=True, style="b green")
        table.add_column("Id", no_wrap=True, style="i")
        if full_doc:
            table.add_column("Documentation", no_wrap=False, style="i")
        else:
            table.add_column("Description", no_wrap=False, style="i")

        for operation_name in sorted(by_type.keys()):
            # if operation_name == "custom_module":
            #     continue

            first_line_value = True
            op_infos = by_type[operation_name]

            for op_id in sorted(op_infos.keys()):
                op_info: OperationInfo = op_infos[op_id]

                if (
                    not include_internal_operations
                    and op_info.operation.operation_details.is_internal_operation
                ):
                    continue

                if full_doc:
                    desc = Markdown(op_info.documentation.full_doc)
                else:
                    desc = Markdown(op_info.documentation.description)

                row: List[RenderableType] = []
                if first_line_value:
                    row.append(operation_name)
                else:
                    row.append("")

                row.append(op_id)
                row.append(desc)

                table.add_row(*row)
                first_line_value = False

        return table


class RendererInfo(ItemInfo):
    renderer_config: Mapping[str, Any] = Field(description="The renderer config.")
    renderer_cls: PythonClass = Field(
        description="The Python class that implements the renderer."
    )
    supported_inputs: List[str] = Field(
        description="Descriptions of the supported inputs."
    )
    supported_source_types: List[str] = Field(
        description="Descriptions of the supported source types."
    )
    supported_target_types: List[str] = Field(
        description="Descriptions of the supported target types."
    )
    supported_python_classes: List[PythonClass] = Field(
        description="A list of supported Python types that are acceptable as inputs."
    )

    @classmethod
    def base_instance_class(cls) -> Type[KiaraRenderer]:
        return KiaraRenderer

    @classmethod
    def create_from_instance(cls, kiara: "Kiara", instance: KiaraRenderer, **kwargs):
        doc = instance.doc
        authors = AuthorsMetadataModel.from_class(instance.__class__)
        properties_md = ContextMetadataModel.from_class(instance.__class__)

        renderer_name = instance._renderer_name  # type: ignore
        renderer_config = instance.renderer_config.model_dump()
        renderer_cls = PythonClass.from_class(item_cls=instance.__class__)
        supported_inputs = list(instance.supported_inputs_descs)
        supported_python_classes = [
            PythonClass.from_class(x)
            for x in instance.retrieve_supported_python_classes()
        ]

        supported_input_types = instance.retrieve_supported_render_sources()
        if isinstance(supported_input_types, str):
            supported_input_types = [supported_input_types]
        supported_target_types = instance.retrieve_supported_render_targets()
        if isinstance(supported_target_types, str):
            supported_target_types = [supported_target_types]

        return cls(
            type_name=renderer_name,
            documentation=doc,
            authors=authors,
            context=properties_md,
            renderer_config=renderer_config,
            renderer_cls=renderer_cls,
            supported_inputs=supported_inputs,
            supported_python_classes=supported_python_classes,
            supported_source_types=supported_input_types,
            supported_target_types=supported_target_types,
        )

    def create_renderable(self, **config: Any) -> RenderableType:
        show_metadata = config.get("show_metadata", False)

        table = Table(box=box.SIMPLE, show_header=False)
        table.add_column("key", style="i")
        table.add_column("value")

        table.add_row("Documentation", self.documentation.create_renderable(**config))
        inputs_md = ""
        for inp in self.supported_inputs:
            inputs_md += f"- {inp}\n"
        table.add_row("Supported inputs", Markdown(inputs_md))

        if show_metadata:
            table.add_row("Renderer name", self.type_name)
            if self.renderer_config:
                json = orjson_dumps(self.renderer_config, option=orjson.OPT_INDENT_2)
                table.add_row(
                    "Renderer config", Syntax(json, "json", background_color="default")
                )

            table.add_row(
                "Renderer class", self.renderer_cls.create_renderable(**config)
            )
            table.add_row("Author(s)", self.authors.create_renderable(**config))
            table.add_row("Context", self.context.create_renderable(**config))

        python_cls_md = ""
        for inp_cls in self.supported_python_classes:
            python_cls_md += f"- {inp_cls.full_name}\n"
        table.add_row(python_cls_md)

        return table


class RendererInfos(InfoItemGroup[RendererInfo]):
    @classmethod
    def base_info_class(cls) -> Type[RendererInfo]:
        return RendererInfo

    def get_render_source_types(self) -> List[str]:
        all_source_types = set()
        item: RendererInfo
        for item in self.item_infos.values():  # type: ignore
            all_source_types.update(item.supported_source_types)

        return sorted(all_source_types)

    def create_renderable(self, **config: Any) -> RenderableType:
        table = Table(
            show_header=True, box=HORIZONTALS_NO_TO_AND_BOTTOM, show_lines=True
        )
        table.add_column("Source type(s)")
        table.add_column("Target type(s)")
        table.add_column("Description")

        rows: Dict[str, Dict[str, List[RenderableType]]] = {}
        info: RendererInfo
        for info in self.item_infos.values():  # type: ignore
            row: List[RenderableType] = []
            source_types = "\n".join(info.supported_source_types)
            target_types = "\n".join(info.supported_target_types)
            row.append(source_types)  # type: ignore
            row.append(target_types)  # type: ignore
            row.append(info.documentation.create_renderable(**config))

            rows.setdefault(source_types, {})[target_types] = row

        for source in sorted(rows.keys()):
            for target in sorted(rows[source].keys()):
                row = rows[source][target]
                table.add_row(*row)

        return table


class KiaraPluginInfo(ItemInfo):
    @classmethod
    def base_instance_class(cls) -> Type[str]:
        return str

    @classmethod
    def create_from_instance(
        cls, kiara: "Kiara", instance: str, **kwargs
    ) -> "KiaraPluginInfo":
        registry = kiara.environment_registry
        python_env: PythonRuntimeEnvironment = registry.environments["python"]  # type: ignore

        match: Union[str, None] = None
        for pkg in python_env.packages:
            pkg_name = pkg.name
            if pkg_name == instance:
                match = pkg.name
            elif pkg_name.startswith("kiara-plugin") or pkg_name.startswith(
                "kiara_plugin"
            ):
                underscored = pkg_name.replace("-", "_")
                if underscored == instance:
                    match = underscored
                    break

        if not match:
            raise KiaraException(
                msg=f"Can't provide information for plugin '{instance}'.",
                reason="Plugin not installed.",
            )

        match = match.replace("kiara-plugin", "kiara_plugin")

        from kiara.utils.operations import filter_operations

        data_types = kiara.type_registry.get_context_metadata(only_for_package=match)
        modules = kiara.module_registry.get_context_metadata(only_for_package=match)
        operation_types = kiara.operation_registry.get_context_metadata(
            only_for_package=match
        )
        operations = filter_operations(
            kiara=kiara, pkg_name=match, **kiara.operation_registry.operations
        )

        model_registry = kiara.kiara_model_registry
        kiara_models = model_registry.get_models_for_package(package_name=match)

        final_pkg_name = match.replace("-", "_")
        base_module = importlib.import_module(final_pkg_name)
        from importlib.metadata import metadata

        pkg_metadata = metadata(final_pkg_name)

        summary = pkg_metadata.get("Summary", None)  # type: ignore[attr-defined]
        desc = pkg_metadata.get("Description", None)  # type: ignore[attr-defined]

        if not summary and not desc:
            doc = DocumentationMetadataModel.create()
        elif not summary:
            doc = DocumentationMetadataModel.create(desc)
        elif not desc:
            doc = DocumentationMetadataModel.create(summary)
        else:
            doc = DocumentationMetadataModel.create(f"{summary}\n\n{desc}")

        def parse_name_email(s):
            match = re.match(r"(.*)\s*<(.+)>", s)
            if match:
                name = match.group(1).strip()
                email = match.group(2).strip()
                return name, email
            else:
                return None, None

        version = pkg_metadata.get("Version")  # type: ignore[attr-defined]
        authors: List[AuthorModel] = []
        for key in pkg_metadata.keys():  # type: ignore[attr-defined]
            if key == "Author-email":
                author, email = parse_name_email(pkg_metadata[key])
                if not author:
                    author = email.split("@")[0]
            elif key == "Author":
                author = pkg_metadata[key]
                email = None
            else:
                continue

            author_obj = AuthorModel(name=author, email=email)
            authors.append(author_obj)

        author_md = AuthorsMetadataModel(authors=authors)

        context = ContextMetadataModel.from_class(base_module)  # type: ignore

        info = KiaraPluginInfo(
            type_name=instance,
            version=version,
            documentation=doc,
            authors=author_md,
            context=context,
            data_types=data_types,
            module_types=modules,
            kiara_model_types=kiara_models,
            operation_types=operation_types,
            operations=operations,
        )
        return info

    version: str = Field(description="The version of the plugin.")
    data_types: DataTypeClassesInfo = Field(description="The included data types.")
    module_types: ModuleTypesInfo = Field(
        description="The included kiara module types."
    )
    kiara_model_types: KiaraModelClassesInfo = Field(
        description="The included model classes."
    )
    # metadata_types: MetadataTypeClassesInfo = Field(
    #     description="The included value metadata types."
    # )
    operation_types: OperationTypeClassesInfo = Field(
        description="The included operation types."
    )
    operations: OperationGroupInfo = Field(description="The included operations.")

    def create_renderable(self, **config: Any) -> RenderableType:
        include_doc = config.get("include_doc", True)
        include_full_doc = config.get("include_full_doc", False)
        include_data_types = config.get("include_data_types", True)
        include_module_types = config.get("include_module_types", True)
        include_operations = config.get("include_operations", True)
        include_operation_types = config.get("include_operation_types", False)
        include_model_types = config.get("include_model_types", False)

        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
        table.add_column("property", style="i")
        table.add_column("value")

        if include_doc:
            if include_full_doc:
                title = "Documentation"
                doc_str = self.documentation.full_doc
            else:
                title = "Description"
                doc_str = self.documentation.description
            table.add_row(
                title,
                Panel(doc_str, box=box.SIMPLE),
            )

        table.add_row("Version", Panel(self.version, box.SIMPLE))
        table.add_row("Author(s)", self.authors.create_renderable())
        table.add_row("Context", self.context.create_renderable())

        if include_data_types:
            table.add_row("data_types", self.data_types.create_renderable(**config))
        if include_module_types:
            table.add_row("module_types", self.module_types.create_renderable(**config))
        if include_operations:
            config_ops = config.copy()
            config_ops["show_internal_column"] = True
            table.add_row("operations", self.operations.create_renderable(**config_ops))
        if include_operation_types:
            table.add_row(
                "operation_types", self.operation_types.create_renderable(**config)
            )
        if include_model_types:
            table.add_row(
                "kiara_model_types", self.kiara_model_types.create_renderable(**config)
            )

        return table


class KiaraPluginInfos(InfoItemGroup[KiaraPluginInfo]):
    @classmethod
    def get_available_plugin_names(
        cls, kiara: "Kiara", regex: Union[str, None] = None
    ) -> List[str]:
        """
        Get a list of all available plugins.

        Arguments:
            regex: an optional regex to indicate the plugin naming scheme (default: /$kiara[_-]plugin\\..*/)

        Returns:
            a list of plugin names
        """

        if regex is None:
            regex = r"^kiara[-_]plugin\..*"

        registry = kiara.environment_registry
        python_env: PythonRuntimeEnvironment = registry.environments["python"]  # type: ignore

        if not regex:
            regex = "^kiara[-_]plugin\\..*"
        regex_c = re.compile(regex)

        result = []
        for pkg in python_env.packages:
            pkg_name = pkg.name
            if pkg_name == "kiara":
                continue

            # check if the package is a kiara plugin
            match = regex_c.search(pkg_name)
            if match:
                result.append(pkg_name)

        return result

    @classmethod
    def base_info_class(cls) -> Type[KiaraPluginInfo]:
        return KiaraPluginInfo

    @classmethod
    def create_group(
        cls,
        kiara: "Kiara",
        group_title: Union[str, None] = None,
        plugin_name_regex: str = "^kiara[-_]plugin\\..*",
    ) -> "KiaraPluginInfos":
        names = cls.get_available_plugin_names(kiara=kiara, regex=plugin_name_regex)
        result = cls.create_from_plugin_names(kiara, group_title, *names)
        return result

    @classmethod
    def create_from_plugin_names(
        cls, kiara: "Kiara", group_title: Union[str, None] = None, *items: str
    ) -> "KiaraPluginInfos":
        """Create the info group from a list of plugin names."""

        plugin_infos = {
            k: KiaraPluginInfo.create_from_instance(kiara=kiara, instance=k)
            for k in items
        }

        op_group_info = cls(group_title=group_title, item_infos=plugin_infos)
        return op_group_info

    def create_renderable(self, **config: Any) -> RenderableType:
        full_doc = config.get("full_doc", False)

        table = Table(show_header=True, box=box.SIMPLE, show_lines=full_doc)
        table.add_column("Name", style="i")
        table.add_column("Version")
        table.add_column("Description")

        for type_name in sorted(self.item_infos.keys()):
            t_md = self.item_infos[type_name]
            version = t_md.version
            if full_doc:
                md = Markdown(t_md.documentation.full_doc)
            else:
                md = Markdown(t_md.documentation.description)
            table.add_row(type_name, version, md)

        return table


class JobInfo(ItemInfo):
    job_record: JobRecord = Field(description="The job record instance.")
    operation: OperationInfo = Field(description="The operation info instance.")
    inputs: Mapping[str, ValueInfo] = Field(description="The inputs.")
    outputs: Mapping[str, ValueInfo] = Field(description="The result(s).")

    @classmethod
    def base_instance_class(cls) -> Type[JobRecord]:
        return JobRecord

    @classmethod
    def create_from_instance(
        cls, kiara: "Kiara", instance: JobRecord, **kwargs
    ) -> "JobInfo":
        type_name = str(instance.job_id)

        module = kiara.module_registry.create_module(instance)
        op = Operation.create_from_module(module=module)
        operation_info: OperationInfo = OperationInfo.create_from_operation(
            kiara=kiara, operation=op
        )
        doc = operation_info.documentation
        authors = operation_info.authors
        context = operation_info.context

        inputs = {}
        for k, v in instance.inputs.items():
            value = kiara.data_registry.get_value(v)
            inputs[k] = ValueInfo.create_from_instance(kiara=kiara, instance=value)

        outputs = {}
        for k, v in instance.outputs.items():
            value = kiara.data_registry.get_value(v)
            outputs[k] = ValueInfo.create_from_instance(kiara=kiara, instance=value)

        return cls(
            type_name=type_name,
            documentation=doc,
            authors=authors,
            context=context,
            job_record=instance,
            operation=operation_info,
            inputs=inputs,
            outputs=outputs,
        )

    def create_renderable(self, **config: Any) -> RenderableType:
        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        table.add_row("Job ID", str(self.job_record.job_id))
        runtime_details = self.job_record.runtime_details
        assert runtime_details is not None
        table.add_row("Job details", runtime_details.create_renderable(**config))
        table.add_row("Operation details", self.operation)

        values_table = Table(show_header=False, box=box.SIMPLE)
        values_table.add_column("field", style="i")
        values_table.add_column("value")
        for k, v in self.inputs.items():
            rendered_value = str(v.value_id)
            values_table.add_row(k, rendered_value)

        table.add_row("Inputs", values_table)

        values_table = Table(show_header=False, box=box.SIMPLE)
        values_table.add_column("field", style="i")
        values_table.add_column("value")
        for k, v in self.outputs.items():
            rendered_value = str(v.value_id)
            values_table.add_row(k, rendered_value)

        table.add_row("Outputs", values_table)

        return table


class JobsInfo(InfoItemGroup[JobInfo]):
    @classmethod
    def base_info_class(cls) -> Type[JobInfo]:
        return JobInfo

    def create_renderable(self, **config: Any) -> RenderableType:
        table = Table(
            show_header=True, box=HORIZONTALS_NO_TO_AND_BOTTOM, show_lines=True
        )
        table.add_column("Job ID")
        table.add_column("Module type")
        table.add_column("Runtime (in seconds)")
        table.add_column("Details")

        info: JobInfo
        for info in self.item_infos.values():  # type: ignore
            runtime_details = info.job_record.runtime_details
            assert runtime_details is not None
            row: List[RenderableType] = []
            row.append(str(info.job_record.job_id))
            row.append(info.operation.operation.module_type)
            row.append(str(runtime_details.runtime))
            row.append(runtime_details.create_renderable(**config))

            table.add_row(*row)

        return table
