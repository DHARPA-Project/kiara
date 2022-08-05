# -*- coding: utf-8 -*-
import orjson
import uuid
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from typing import TYPE_CHECKING, Any, Iterable, List, Literal, Mapping, Type, Union

from kiara.interfaces.python_api.models.info import (
    InfoItemGroup,
    ItemInfo,
    TypeInfo,
    TypeInfoItemGroup,
)
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.python_class import PythonClass
from kiara.registries import ArchiveDetails, KiaraArchive
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from kiara.context import Kiara


class ArchiveTypeInfo(TypeInfo):

    _kiara_model_id = "info.archive_type"

    @classmethod
    def create_from_type_class(
        self, type_cls: Type[KiaraArchive], kiara: "Kiara"
    ) -> "ArchiveTypeInfo":

        authors_md = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClass.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)
        type_name = type_cls._archive_type_name  # type: ignore

        return ArchiveTypeInfo.construct(
            type_name=type_name,
            documentation=doc,
            authors=authors_md,
            context=properties_md,
            python_class=python_class,
            supported_item_types=list(type_cls.supported_item_types()),
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


class ArchiveTypeClassesInfo(TypeInfoItemGroup):

    _kiara_model_id = "info.archive_types"

    @classmethod
    def base_info_class(cls) -> Type[ArchiveTypeInfo]:
        return ArchiveTypeInfo

    type_name: Literal["archive_type"] = "archive_type"
    item_infos: Mapping[str, ArchiveTypeInfo] = Field(  # type: ignore
        description="The archive info instances for each type."
    )


class ArchiveInfo(ItemInfo):
    @classmethod
    def base_instance_class(cls) -> Type[KiaraArchive]:
        return KiaraArchive

    @classmethod
    def create_from_instance(cls, kiara: "Kiara", instance: KiaraArchive, **kwargs):

        return cls.create_from_archive(kiara=kiara, archive=instance, **kwargs)

    @classmethod
    def create_from_archive(
        cls,
        kiara: "Kiara",
        archive: KiaraArchive,
        archive_aliases: Union[Iterable[str], None] = None,
    ):

        archive_type_info = ArchiveTypeInfo.create_from_type_class(
            archive.__class__, kiara=kiara
        )
        if archive_aliases is None:
            archive_aliases = []
        else:
            archive_aliases = list(archive_aliases)
        return ArchiveInfo(
            archive_type_info=archive_type_info,
            type_name=str(archive.archive_id),
            documentation=archive_type_info.documentation,
            authors=archive_type_info.authors,
            context=archive_type_info.context,
            archive_id=archive.archive_id,
            details=archive.get_archive_details(),
            config=archive.config.dict(),
            aliases=archive_aliases,
        )

    @classmethod
    def category_name(cls) -> str:
        return "info.archive"

    archive_id: uuid.UUID = Field(description="The (globally unique) archive id.")
    archive_type_info: ArchiveTypeInfo = Field(
        description="Information about this archives' type."
    )
    config: Mapping[str, Any] = Field(description="The configuration of this archive.")
    details: ArchiveDetails = Field(
        description="Type dependent (runtime) details for this archive."
    )
    aliases: List[str] = Field(
        description="Aliases for this archive.", default_factory=list
    )


class ArchiveGroupInfo(InfoItemGroup):

    _kiara_model_id = "info.archives"

    @classmethod
    def base_info_class(cls) -> Type[ItemInfo]:
        return ArchiveInfo

    @classmethod
    def create_from_context(
        cls, kiara: "Kiara", group_title: Union[str, None] = None
    ) -> "ArchiveGroupInfo":

        archives = {}
        for archive, aliases in kiara.get_all_archives().items():
            archives[str(archive.archive_id)] = ArchiveInfo.create_from_archive(
                kiara=kiara, archive=archive, archive_aliases=aliases
            )

        info = cls(group_title=group_title, item_infos=archives)
        return info

    item_infos: Mapping[str, ArchiveInfo] = Field(
        description="The info for each archive."
    )

    @property
    def combined_size(self) -> int:

        combined = 0
        for archive_info in self.item_infos.values():
            size = archive_info.details.size
            if size and size > 0:
                combined = combined + size

        return combined

    def create_renderable(self, **config: Any) -> RenderableType:

        show_archive_id = config.get("show_archive_id", False)
        show_config = config.get("show_config", True)
        show_details = config.get("show_details", False)

        # by_type: Dict[str, Dict[str, ArchiveInfo]] = {}
        # for archive_id, archive in sorted(self.item_infos.items()):
        #     for item_type in archive.archive_type_info.supported_item_types:
        #         by_type.setdefault(item_type, {})[archive.type_name] = archive

        table = Table(show_header=True, box=box.SIMPLE)
        if show_archive_id:
            table.add_column("archive id")
        table.add_column("alias(es)", style="i")
        table.add_column("item type(s)", style="i")
        if show_config:
            table.add_column("config")
        if show_details:
            table.add_column("details")

        for archive in self.item_infos.values():
            row: List[RenderableType] = []
            if show_archive_id:
                row.append(str(archive.archive_id))
            row.append("\n".join(archive.aliases))
            row.append("\n".join(archive.archive_type_info.supported_item_types))

            if show_config:
                config_json = Syntax(
                    orjson_dumps(archive.config, option=orjson.OPT_INDENT_2),
                    "json",
                    background_color="default",
                )
                row.append(config_json)
            if show_details:
                details_json = Syntax(
                    orjson_dumps(archive.details, option=orjson.OPT_INDENT_2),
                    "json",
                    background_color="default",
                )
                row.append(details_json)

            table.add_row(*row)

        return table
