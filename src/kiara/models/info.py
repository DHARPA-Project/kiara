# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import orjson
from pydantic import Field, validator
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterable,
    Literal,
    Mapping,
    Optional,
    Type,
    TypeVar,
)

from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.python_class import PythonClass
from kiara.utils import orjson_dumps
from kiara.utils.class_loading import find_all_kiara_model_classes

if TYPE_CHECKING:
    pass

INFO_BASE_CLASS = TypeVar("INFO_BASE_CLASS")


class ItemInfo(KiaraModel):
    """Base class that holds/manages information about an item within kiara."""

    @classmethod
    @abc.abstractmethod
    def category_name(cls) -> str:
        pass

    @validator("documentation", pre=True)
    def validate_doc(cls, value):

        return DocumentationMetadataModel.create(value)

    type_name: str = Field(description="The registered name for this item type.")
    documentation: DocumentationMetadataModel = Field(
        description="Documentation for the module."
    )
    authors: AuthorsMetadataModel = Field(
        description="Information about authorship for the module type."
    )
    context: ContextMetadataModel = Field(
        description="Generic properties of this module (description, tags, labels, references, ...)."
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
    @abc.abstractmethod
    def create_from_type_class(self, type_cls: Type[INFO_BASE_CLASS]) -> "ItemInfo":
        pass

    @classmethod
    @abc.abstractmethod
    def base_class(self) -> Type[INFO_BASE_CLASS]:
        pass

    python_class: PythonClass = Field(
        description="The python class that implements this module type."
    )


INFO_CLASS = TypeVar("INFO_CLASS", bound=ItemInfo)


class InfoModelGroup(KiaraModel, Mapping[str, ItemInfo]):
    @classmethod
    @abc.abstractmethod
    def base_info_class(cls) -> Type[ItemInfo]:
        pass

    # group_id: uuid.UUID = Field(
    #     description="The unique group id.", default_factory=ID_REGISTRY.generate
    # )
    group_alias: Optional[str] = Field(description="The group alias.", default=None)

    # def _retrieve_id(self) -> str:
    #     return str(self.group_id)

    def _retrieve_subcomponent_keys(self) -> Iterable[str]:
        return self.item_infos.keys()  # type: ignore

    def _retrieve_data_to_hash(self) -> Any:
        return {"type_name": self.type_name, "included_types": list(self.item_infos.keys())}  # type: ignore

    def get_item_infos(self) -> Mapping[str, ItemInfo]:
        return self.item_infos  # type: ignore

    def create_renderable(self, **config: Any) -> RenderableType:

        full_doc = config.get("full_doc", False)

        table = Table(show_header=True, box=box.SIMPLE, show_lines=full_doc)
        table.add_column("Name", style="i")
        table.add_column("Description")

        for type_name in sorted(self.item_infos.keys()):  # type: ignore
            t_md = self.item_infos[type_name]  # type: ignore
            if full_doc:
                md = Markdown(t_md.documentation.full_doc)
            else:
                md = Markdown(t_md.documentation.description)
            table.add_row(type_name, md)

        return table

    def __getitem__(self, item: str) -> ItemInfo:

        return self.get_item_infos()[item]

    def __iter__(self):
        return iter(self.get_item_infos())

    def __len__(self):
        return len(self.get_item_infos())


class TypeInfoModelGroup(InfoModelGroup, Mapping[str, TypeInfo]):
    @classmethod
    @abc.abstractmethod
    def base_info_class(cls) -> Type[TypeInfo]:
        pass

    @classmethod
    def create_from_type_items(
        cls, group_alias: Optional[str] = None, **items: Type
    ) -> "TypeInfoModelGroup":

        type_infos = {
            k: cls.base_info_class().create_from_type_class(v) for k, v in items.items()
        }
        data_types_info = cls.construct(group_alias=group_alias, item_infos=type_infos)  # type: ignore
        return data_types_info

    def get_item_infos(self) -> Mapping[str, TypeInfo]:
        return self.item_infos  # type: ignore

    def __getitem__(self, item: str) -> TypeInfo:

        return self.get_item_infos()[item]

    def __iter__(self):
        return iter(self.get_item_infos())

    def __len__(self):
        return len(self.get_item_infos())


class KiaraModelTypeInfo(TypeInfo):

    _kiara_model_id = "info.kiara_model"

    @classmethod
    def create_from_type_class(
        self, type_cls: Type[KiaraModel]
    ) -> "KiaraModelTypeInfo":

        authors_md = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClass.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)
        type_name = type_cls._kiara_model_id  # type: ignore
        schema = type_cls.schema()

        return KiaraModelTypeInfo.construct(
            type_name=type_name,
            documentation=doc,
            authors=authors_md,
            context=properties_md,
            python_class=python_class,
            metadata_schema=schema,
        )

    @classmethod
    def base_class(self) -> Type[KiaraModel]:
        return KiaraModel

    @classmethod
    def category_name(cls) -> str:
        return "info.kiara_model"

    metadata_schema: Dict[str, Any] = Field(
        description="The (json) schema for this model data."
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


class KiaraModelClassesInfo(TypeInfoModelGroup):

    _kiara_model_id = "info.kiara_models"

    @classmethod
    def base_info_class(cls) -> Type[TypeInfo]:
        return KiaraModelTypeInfo

    type_name: Literal["kiara_model"] = "kiara_model"
    item_infos: Mapping[str, KiaraModelTypeInfo] = Field(
        description="The value metadata info instances for each type."
    )


def find_kiara_models(
    alias: Optional[str] = None, only_for_package: Optional[str] = None
) -> KiaraModelClassesInfo:

    models = find_all_kiara_model_classes()

    group: KiaraModelClassesInfo = KiaraModelClassesInfo.create_from_type_items(group_alias=alias, **models)  # type: ignore

    if only_for_package:
        temp = {}
        for key, info in group.items():
            if info.context.labels.get("package") == only_for_package:
                temp[key] = info

        group = KiaraModelClassesInfo.construct(
            group_id=group.instance_id, group_alias=group.group_alias, item_infos=temp  # type: ignore
        )

    return group
