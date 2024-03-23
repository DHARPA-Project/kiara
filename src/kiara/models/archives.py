# -*- coding: utf-8 -*-
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    List,
    Literal,
    Mapping,
    Type,
    Union,
)

import orjson
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from kiara.interfaces.python_api.models.info import (
    InfoItemGroup,
    ItemInfo,
    TypeInfo,
    TypeInfoItemGroup,
)
from kiara.models.documentation import (
    AuthorModel,
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.python_class import PythonClass
from kiara.registries import ArchiveDetails, ArchiveMetadata, KiaraArchive
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from kiara.api import KiArchive
    from kiara.context import Kiara


class ArchiveTypeInfo(TypeInfo):

    _kiara_model_id: ClassVar = "info.archive_type"

    @classmethod
    def create_from_type_class(
        self, type_cls: Type[KiaraArchive], kiara: "Kiara"
    ) -> "ArchiveTypeInfo":

        authors_md = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClass.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)
        type_name = type_cls._archive_type_name  # type: ignore

        return ArchiveTypeInfo(
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

        # table.add_row("is_writeable", "yes" if self.is_writable else "no")
        table.add_row(
            "supported_item_types", ", ".join(sorted(self.supported_item_types))
        )

        return table


class ArchiveTypeClassesInfo(TypeInfoItemGroup):

    _kiara_model_id: ClassVar = "info.archive_types"

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

        if kwargs:
            raise ValueError("kwargs not supported.")

        return cls.create_from_archive(kiara=kiara, archive=instance)

    @classmethod
    def create_from_archive(
        cls,
        kiara: "Kiara",
        archive: KiaraArchive,
        # archive_aliases: Union[Iterable[str], None] = None,
    ):

        doc_str = archive.archive_metadata.get("description", None)
        doc = DocumentationMetadataModel.create(doc_str)

        authors_raw = archive.archive_metadata.get("authors", [])
        _authors = []
        for author in authors_raw:
            author = AuthorModel(**author)
            _authors.append(author)
        authors = AuthorsMetadataModel(authors=_authors)

        tags = archive.archive_metadata.get("tags", [])
        labels = archive.archive_metadata.get("labels", {})

        references = archive.archive_metadata.get("references", {})
        # TODO: add references model

        context = ContextMetadataModel(tags=tags, labels=labels, references=references)

        # archive_types = list(archive.supported_item_types())

        archive_type_info = ArchiveTypeInfo.create_from_type_class(
            archive.__class__, kiara=kiara
        )
        # if archive_aliases is None:
        #     archive_aliases = []
        # else:
        #     archive_aliases = list(archive_aliases)
        return ArchiveInfo(
            archive_type_info=archive_type_info,
            archive_alias=archive.archive_name,
            archive_id=archive.archive_id,
            type_name=str(archive.archive_id),
            documentation=doc,
            authors=authors,
            context=context,
            # archive_types=archive_types,
            details=archive.get_archive_details(),
            metadata=archive.archive_metadata,
            config=archive.config.model_dump(),
            # aliases=archive_aliases,
        )

    @classmethod
    def category_name(cls) -> str:
        return "info.archive"

    archive_id: uuid.UUID = Field(description="The (globally unique) archive id.")
    archive_alias: str = Field(description="The archive alias.")

    archive_type_info: ArchiveTypeInfo = Field(
        description="Information about this archives' type."
    )
    # archive_types: List[Literal["data", "alias", "job_record", "workflow"]] = Field(description="The archive type.")

    config: Mapping[str, Any] = Field(description="The configuration of this archive.")
    details: ArchiveDetails = Field(
        description="Type dependent (runtime) details for this archive."
    )
    metadata: ArchiveMetadata = Field(description="Metadata for this archive.")
    # aliases: List[str] = Field(
    #     description="Aliases for this archive.", default_factory=list
    # )

    def create_renderable(self, **config: Any) -> RenderableType:
        from kiara.utils.output import extract_renderable

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("property", style="i")
        table.add_column("value")

        details = extract_renderable(self.details, render_config=config)
        metadata = extract_renderable(self.metadata, render_config=config)
        type_info = self.archive_type_info.create_renderable(**config)
        # table.add_row("archive id", str(self.archive_id))
        # table.add_row("archive alias", self.archive_alias)
        # table.add_row("archive type(s)", ", ".join(self.archive_types))
        table.add_row("details", details)
        table.add_row("metadata", metadata)
        table.add_row("archive type info", type_info)
        if self.documentation.is_set:
            table.add_row("doc", self.documentation.create_renderable(**config))
        if self.authors.authors:
            table.add_row("author(s)", self.authors.create_renderable(**config))
        if self.context.labels or self.context.tags or self.context.references:
            table.add_row("context", self.context.create_renderable(**config))

        return table


class ArchiveGroupInfo(InfoItemGroup):

    _kiara_model_id: ClassVar = "info.archives"

    @classmethod
    def base_info_class(cls) -> Type[ItemInfo]:
        return ArchiveInfo

    @classmethod
    def create_from_context(
        cls, kiara: "Kiara", group_title: Union[str, None] = None
    ) -> "ArchiveGroupInfo":

        archives = {}
        for archive, aliases in kiara.get_all_archives().items():
            title = str(archive.archive_id) + ", ".join(aliases)
            archives[title] = ArchiveInfo.create_from_archive(
                kiara=kiara, archive=archive
            )
            # archives[str(archive.archive_id)] = ArchiveInfo.create_from_archive(
            #     kiara=kiara, archive=archive, archive_aliases=aliases
            # )

        info = cls(group_title=group_title, item_infos=archives)
        return info

    item_infos: Mapping[str, ArchiveInfo] = Field(
        description="The info for each archive."
    )

    @property
    def combined_size(self) -> int:

        combined = 0
        archive_ids = set()
        for archive_info in self.item_infos.values():
            if archive_info.archive_id in archive_ids:
                continue
            archive_ids.add(archive_info.archive_id)
            size = archive_info.details.root.get("size", 0)
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
        table.add_column("alias", style="i")
        table.add_column("item type(s)", style="i")
        table.add_column("archive type", style="i")
        if show_config:
            table.add_column("config")
        if show_details:
            table.add_column("details")

        for archive in self.item_infos.values():
            row: List[RenderableType] = []
            if show_archive_id:
                row.append(str(archive.archive_id))
            row.append(archive.archive_alias)
            row.append("\n".join(archive.archive_type_info.supported_item_types))
            row.append(archive.archive_type_info.type_name)

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


class KiArchiveInfo(ItemInfo):
    @classmethod
    def base_instance_class(cls) -> Type["KiArchive"]:
        from kiara.api import KiArchive

        return KiArchive

    @classmethod
    def create_from_instance(
        cls, kiara: "Kiara", instance: "KiArchive", **kwargs
    ) -> "KiArchiveInfo":

        return cls.create_from_kiarchive(kiarchive=instance, **kwargs)

    @classmethod
    def create_from_kiarchive(cls, kiarchive: "KiArchive") -> "KiArchiveInfo":

        data_archive = kiarchive.data_archive
        alias_archive = kiarchive.alias_archive
        job_archive = kiarchive.job_archive
        metadata_archive = kiarchive.metadata_archive

        data_archive_info = None
        alias_archive_info = None
        job_archive_info = None
        metadata_archive_info = None

        documentation: Union[DocumentationMetadataModel, None] = None
        authors: Union[AuthorsMetadataModel, None] = None
        context: Union[ContextMetadataModel, None] = None

        _kiara = kiarchive._kiara
        if _kiara is None:
            raise ValueError("No kiara instance attached to kiarchive instance.")

        if data_archive:
            data_archive_info = ArchiveInfo.create_from_archive(
                kiara=_kiara, archive=data_archive
            )
            documentation = data_archive_info.documentation
            authors = data_archive_info.authors
            context = data_archive_info.context

        if alias_archive:
            alias_archive_info = ArchiveInfo.create_from_archive(
                kiara=_kiara, archive=alias_archive
            )
            # TODO: should we separate those per archive?
            documentation = alias_archive_info.documentation
            authors = alias_archive_info.authors
            context = alias_archive_info.context

        if metadata_archive:
            metadata_archive_info = ArchiveInfo.create_from_archive(
                kiara=_kiara, archive=metadata_archive
            )
            documentation = metadata_archive_info.documentation
            authors = metadata_archive_info.authors
            context = metadata_archive_info.context

        if job_archive:
            job_archive_info = ArchiveInfo.create_from_archive(
                kiara=_kiara, archive=job_archive
            )
            documentation = job_archive_info.documentation
            authors = job_archive_info.authors
            context = job_archive_info.context

        if documentation is None or authors is None or context is None:
            raise ValueError("No documentation, authors or context found.")

        return KiArchiveInfo(
            type_name=kiarchive.archive_file_name,
            data_archive_info=data_archive_info,
            alias_archive_info=alias_archive_info,
            metadata_archive_info=metadata_archive_info,
            job_archive_info=job_archive_info,
            documentation=documentation,
            authors=authors,
            context=context,
        )

    data_archive_info: Union[ArchiveInfo, None] = Field(
        description="The info for the included data archive."
    )
    alias_archive_info: Union[ArchiveInfo, None] = Field(
        description="The info for the included alias archive."
    )
    metadata_archive_info: Union[ArchiveInfo, None] = Field(
        description="The info for the included metadata archive."
    )
    job_archive_info: Union[ArchiveInfo, None] = Field(
        description="The info for the included job archive."
    )

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("property", style="i")
        table.add_column("value")

        if self.data_archive_info:
            content = self.data_archive_info.create_renderable(**config)
        else:
            content = "-- no data archive --"
        table.add_row("data archive", content)

        if self.alias_archive_info:
            content = self.alias_archive_info.create_renderable(**config)
        else:
            content = "-- no alias archive --"
        table.add_row("alias archive", content)

        if self.metadata_archive_info:
            content = self.metadata_archive_info.create_renderable(**config)
        else:
            content = "-- no metadata archive --"
        table.add_row("metadata archive", content)

        if self.job_archive_info:
            content = self.job_archive_info.create_renderable(**config)
        else:
            content = "-- no job archive --"
        table.add_row("job archive", content)

        return table
