# -*- coding: utf-8 -*-
import typing
from pydantic import BaseModel
from rich import box
from rich.console import (
    Console,
    ConsoleOptions,
    RenderableType,
    RenderGroup,
    RenderResult,
)
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table

from kiara.utils import merge_dicts
from kiara.utils.doc import extract_doc_from_cls
from kiara.utils.output import first_line

if typing.TYPE_CHECKING:
    from kiara import Kiara


class MetadataModel(BaseModel):
    @classmethod
    def model_doc(cls) -> str:

        return extract_doc_from_cls(cls)

    @classmethod
    def model_desc(cls) -> str:
        return extract_doc_from_cls(cls, only_first_line=True)

    @classmethod
    def from_dicts(cls, *dicts: typing.Mapping[str, typing.Any]):

        if not dicts:
            return cls()

        merged = merge_dicts(*dicts)
        return cls.parse_obj(merged)

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for k in self.__fields__.keys():
            value_str = str(getattr(self, k))
            table.add_row(k, value_str)
        return table

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield self.create_renderable()


class MetadataSchemaInfo(object):
    def __init__(
        self, model_cls: typing.Type[MetadataModel], display_schema: bool = False
    ):
        self._model_cls: typing.Type[MetadataModel] = model_cls
        self._display_schema: bool = display_schema

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("Field name", style="i")
        table.add_column("Type")
        table.add_column("Required")
        table.add_column("Description")

        for field_name, details in self._model_cls.__fields__.items():
            field_type = self._model_cls.schema()["properties"][field_name]["type"]
            req = "yes" if details.required else "no"
            info = details.field_info.description
            table.add_row(field_name, field_type, req, info)

        md = Markdown(self._model_cls.model_doc())
        rg_list: typing.List[typing.Any] = [md]

        rg_list.append(table)

        if self._display_schema:
            md_string = f"\n**JSON schema for ``{self._model_cls._metadata_key}``:**\n\n```\n{self._model_cls.schema_json(indent=2)}\n```"  # type: ignore
            schema_md = Markdown(md_string)
            rg_list.append(schema_md)

        rg = RenderGroup(*rg_list)

        panel = Panel(rg, title=f"Metadata schema: [b]{self._model_cls._metadata_key}[/b]", title_align="left", padding=(1, 1))  # type: ignore
        yield panel


class MetadataSet(object):
    def __init__(
        self,
        kiara: "Kiara",
        subject: typing.Any,
        **metadata: typing.Mapping[str, typing.Any],
    ):

        self._kiara: "Kiara" = kiara
        self._data: typing.Dict[str, typing.Mapping[str, typing.Any]] = metadata
        self._metadata: typing.Dict[str, MetadataModel] = {}
        self._subject: typing.Any = subject

    def get_metadata(self, metadata_key: str) -> MetadataModel:

        if metadata_key in self._metadata.keys():
            return self._metadata[metadata_key]

        if metadata_key not in self._data.keys():
            raise Exception(f"No metadata for key '{metadata_key}' available.")

        md = self._data[metadata_key]
        schema = self._kiara.metadata_mgmt.all_schemas.get(metadata_key, None)
        if schema is None:
            raise Exception(
                f"No metadata schema for key '{metadata_key}' registered in kiara."
            )

        metadata_model_obj = schema(**md)
        self._metadata[metadata_key] = metadata_model_obj
        return self._metadata[metadata_key]

    def get_schema(self, metadata_key) -> MetadataSchemaInfo:
        pass


class MetadataSchemasInfo(object):
    def __init__(
        self, metadata_schemas: typing.Mapping[str, typing.Type[MetadataModel]]
    ):

        self._schemas: typing.Mapping[
            str, typing.Type[MetadataModel]
        ] = metadata_schemas

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Schema name", style="b")
        table.add_column("Description", style="i")

        for name, schema in self._schemas.items():
            table.add_row(name, first_line(schema.model_doc()))

        panel = Panel(table, title="Available schemas", title_align="left")  # type: ignore
        yield panel
