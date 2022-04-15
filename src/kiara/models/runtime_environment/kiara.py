# -*- coding: utf-8 -*-

#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.table import Table
from typing import Any, Dict, List, Literal, Mapping, Optional, Type

from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.info import KiaraTypeInfoModel, TypeInfoModelGroup
from kiara.models.python_class import PythonClass
from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.value_metadata import MetadataTypeClassesInfo
from kiara.registries import KiaraArchive
from kiara.utils.class_loading import find_all_archive_types
from kiara.utils.metadata import find_metadata_models


class ArchiveTypeInfoModel(KiaraTypeInfoModel):
    @classmethod
    def create_from_type_class(
        self, type_cls: Type[KiaraArchive]
    ) -> "ArchiveTypeInfoModel":

        authors_md = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClass.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)
        type_name = type_cls._archive_type_name  # type: ignore

        return ArchiveTypeInfoModel.construct(
            type_name=type_name,
            documentation=doc,
            authors=authors_md,
            context=properties_md,
            python_class=python_class,
        )

    @classmethod
    def base_class(self) -> Type[KiaraArchive]:
        return KiaraArchive

    @classmethod
    def category_name(cls) -> str:
        return "archive_type"

    is_writable: bool = Field(
        description="Whether this archive is writeable.", default=False
    )
    supported_item_types: List[str] = Field(
        description="The item types this archive suports."
    )

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

        table.add_row("Python class", self.python_class.create_renderable())

        table.add_row("is_writeable", "yes" if self.is_writable else "no")
        table.add_row(
            "supported_item_types", ", ".join(sorted(self.supported_item_types))
        )

        return table


class ArchiveTypeClassesInfo(TypeInfoModelGroup):
    @classmethod
    def base_info_class(cls) -> Type[ArchiveTypeInfoModel]:
        return ArchiveTypeInfoModel

    type_name: Literal["archive_type"] = "archive_type"
    type_infos: Mapping[str, ArchiveTypeInfoModel] = Field(
        description="The archive info instances for each type."
    )


def find_archive_types(
    alias: Optional[str] = None, only_for_package: Optional[str] = None
) -> ArchiveTypeClassesInfo:

    archive_types = find_all_archive_types()

    group: ArchiveTypeClassesInfo = ArchiveTypeClassesInfo.create_from_type_items(  # type: ignore
        group_alias=alias, **archive_types
    )

    if only_for_package:
        temp: Dict[str, KiaraTypeInfoModel] = {}
        for key, info in group.items():
            if info.context.labels.get("package") == only_for_package:
                temp[key] = info  # type: ignore

        group = ArchiveTypeClassesInfo.construct(
            group_id=group.group_id, group_alias=group.group_alias, type_infos=temp  # type: ignore
        )

    return group


class KiaraTypesRuntimeEnvironment(RuntimeEnvironment):

    environment_type: Literal["kiara_types"]
    archive_types: ArchiveTypeClassesInfo = Field(
        description="The available implemented store types."
    )
    metadata_types: MetadataTypeClassesInfo = Field(
        description="The available metadata types."
    )

    @classmethod
    def retrieve_environment_data(cls) -> Dict[str, Any]:

        result: Dict[str, Any] = {}
        result["metadata_types"] = find_metadata_models()
        result["archive_types"] = find_archive_types()

        return result
