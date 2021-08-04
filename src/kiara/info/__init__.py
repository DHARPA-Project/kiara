# -*- coding: utf-8 -*-
import typing
from pydantic import BaseModel
from rich import box
from rich.console import (
    Console,
    ConsoleOptions,
    ConsoleRenderable,
    RenderableType,
    RenderGroup,
    RenderResult,
    RichCast,
)
from rich.jupyter import JupyterMixin
from rich.panel import Panel
from rich.table import Table


def extract_renderable(item: typing.Any):

    if isinstance(item, (ConsoleRenderable, RichCast, str)):
        return item
    elif isinstance(item, typing.Mapping):
        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for k, v in item.items():
            table.add_row(k, extract_renderable(v))
        return table
    elif isinstance(item, typing.Iterable):
        _all = []
        for i in item:
            _all.append(extract_renderable(i))
        rg = RenderGroup(*_all)
        return rg
    else:
        return str(item)


class KiaraInfoModel(BaseModel, JupyterMixin):
    def create_panel(self, title: str = None, **config: typing.Any) -> Panel:

        rend = self.create_renderable(**config)
        return Panel(rend, box=box.ROUNDED, title=title, title_align="left")

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for k in self.__fields__.keys():
            attr = getattr(self, k)
            v = extract_renderable(attr)
            table.add_row(k, v)
        return table

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield self.create_renderable()
