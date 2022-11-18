# -*- coding: utf-8 -*-
import humanfriendly
import structlog
from pydantic import BaseModel, Field
from rich import box
from rich.console import Console, ConsoleOptions, RenderableType, RenderResult
from rich.panel import Panel
from rich.table import Table
from typing import Any, Dict, List, Union

from kiara.models.values.value import PersistedData, Value

logger = structlog.getLogger()


class StoreValueResult(BaseModel):

    value: Value = Field(description="The stored value.")
    aliases: List[str] = Field(
        description="The aliases that where assigned to the value when stored."
    )
    persisted_data: Union[None, PersistedData] = Field(
        description="The structure describing the data that was persisted, 'None' if the data was already stored before (or storing failed)."
    )
    error: Union[str, None] = Field(
        description="An error that occured while trying to store."
    )

    def _repr_html_(self):

        r = self.create_renderable()
        mime_bundle = r._repr_mimebundle_(include=[], exclude=[])  # type: ignore
        return mime_bundle["text/html"]

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield self.create_renderable()

    def create_renderable(self, **config) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", "i")
        table.add_column("value")

        table.add_row("value_id", str(self.value.value_id))
        if self.aliases:
            if len(self.aliases) > 1:
                a = "aliases"
            else:
                a = "alias"
            table.add_row(a, ", ".join(self.aliases))
        else:
            table.add_row("aliases", "-- no aliases --")
        table.add_row("data type", self.value.data_type_name)
        table.add_row("size", humanfriendly.format_size(self.value.value_size))
        table.add_row("success", "yes" if not self.error else "no")
        if self.error:
            table.add_row("[red]error[/red]", f"{self.error}")

        return Panel(table, title="Store operation result", title_align="left")


class StoreValuesResult(BaseModel):

    __root__: Dict[str, StoreValueResult]

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=True, show_lines=False, box=box.SIMPLE)
        table.add_column("field", style="b")
        table.add_column("data type", style="i")
        table.add_column("stored id", style="i")
        table.add_column("alias(es)")

        for field_name, value_result in self.__root__.items():
            row = [
                field_name,
                str(value_result.value.value_schema.type),
                str(value_result.value.value_id),
            ]
            if value_result.aliases:
                row.append(", ".join(value_result.aliases))
            else:
                row.append("")
            table.add_row(*row)

        return table

    def __len__(self):
        return len(self.__root__)
