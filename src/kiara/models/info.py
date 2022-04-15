# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import uuid
from pydantic import Field, validator
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
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
from kiara.registries.ids import ID_REGISTRY

if TYPE_CHECKING:
    pass

INFO_BASE_CLASS = TypeVar("INFO_BASE_CLASS")


class KiaraInfoModel(KiaraModel, Generic[INFO_BASE_CLASS]):
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

    def _retrieve_category_id(self) -> str:
        return f"type_info.{self.__class__.category_name()}"

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


class KiaraTypeInfoModel(KiaraInfoModel, Generic[INFO_BASE_CLASS]):
    @classmethod
    @abc.abstractmethod
    def create_from_type_class(
        self, type_cls: Type[INFO_BASE_CLASS]
    ) -> "KiaraInfoModel":
        pass

    @classmethod
    @abc.abstractmethod
    def base_class(self) -> Type[INFO_BASE_CLASS]:
        pass

    python_class: PythonClass = Field(
        description="The python class that implements this module type."
    )


INFO_CLASS = TypeVar("INFO_CLASS", bound=KiaraInfoModel)


class InfoModelGroup(KiaraModel, Mapping[str, KiaraInfoModel]):
    @classmethod
    @abc.abstractmethod
    def base_info_class(cls) -> Type[KiaraInfoModel]:
        pass

    group_id: uuid.UUID = Field(
        description="The unique group id.", default_factory=ID_REGISTRY.generate
    )
    group_alias: Optional[str] = Field(description="The group alias.", default=None)

    def _retrieve_id(self) -> str:
        return str(self.group_id)

    def _retrieve_category_id(self) -> str:
        return f"type_info_group.{self.type_name}"  # type: ignore

    def _retrieve_subcomponent_keys(self) -> Iterable[str]:
        return self.type_infos.keys()  # type: ignore

    def _retrieve_data_to_hash(self) -> Any:
        return {"type_name": self.type_name, "included_types": self.type_infos.keys()}  # type: ignore

    def get_type_infos(self) -> Mapping[str, KiaraInfoModel]:
        return self.type_infos  # type: ignore

    def create_renderable(self, **config: Any) -> RenderableType:

        full_doc = config.get("full_doc", False)

        table = Table(show_header=True, box=box.SIMPLE, show_lines=full_doc)
        table.add_column("Type name", style="i")
        table.add_column("Description")

        for type_name in sorted(self.type_infos.keys()):  # type: ignore
            t_md = self.type_infos[type_name]  # type: ignore
            if full_doc:
                md = Markdown(t_md.documentation.full_doc)
            else:
                md = Markdown(t_md.documentation.description)
            table.add_row(type_name, md)

        return table

    def __getitem__(self, item: str) -> KiaraInfoModel:

        return self.get_type_infos()[item]

    def __iter__(self):
        return iter(self.get_type_infos())

    def __len__(self):
        return len(self.get_type_infos())


class TypeInfoModelGroup(InfoModelGroup, Mapping[str, KiaraTypeInfoModel]):
    @classmethod
    @abc.abstractmethod
    def base_info_class(cls) -> Type[KiaraTypeInfoModel]:
        pass

    @classmethod
    def create_from_type_items(
        cls, group_alias: Optional[str] = None, **items: Type
    ) -> "TypeInfoModelGroup":

        type_infos = {
            k: cls.base_info_class().create_from_type_class(v) for k, v in items.items()
        }
        data_types_info = cls.construct(group_alias=group_alias, type_infos=type_infos)  # type: ignore
        return data_types_info

    def get_type_infos(self) -> Mapping[str, KiaraTypeInfoModel]:
        return self.type_infos  # type: ignore

    def __getitem__(self, item: str) -> KiaraTypeInfoModel:

        return self.get_type_infos()[item]

    def __iter__(self):
        return iter(self.get_type_infos())

    def __len__(self):
        return len(self.get_type_infos())
