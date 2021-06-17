# -*- coding: utf-8 -*-
import typing
from inspect import cleandoc
from pydantic import BaseModel
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.panel import Panel
from rich.table import Table

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.utils.output import first_line


class MetadataModel(BaseModel):
    @classmethod
    def doc(cls) -> str:
        doc = cls.__doc__
        if not doc:
            doc = DEFAULT_NO_DESC_VALUE
        else:
            doc = cleandoc(doc)

        return doc.strip()


class MetadataSchemaInfo(object):
    def __init__(self, model_cls: typing.Type[MetadataModel]):
        self._model_cls: typing.Type[MetadataModel] = model_cls

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

        panel = Panel(table, title=f"Metadata schema: [b]{self._model_cls._metadata_key}[/b]", title_align="left")  # type: ignore
        yield panel


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
            table.add_row(name, first_line(schema.doc()))

        panel = Panel(table, title="Available schemas", title_align="left")  # type: ignore
        yield panel
