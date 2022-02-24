# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import networkx as nx
import typing
from pydantic import BaseModel, PrivateAttr
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

    _graph_cache: typing.Optional[nx.DiGraph] = PrivateAttr(default=None)
    _subcomponent_names_cache: typing.Union[None, bool, typing.List[str]] = PrivateAttr(
        default=None
    )
    _dynamic_subcomponents: typing.Dict[str, "KiaraInfoModel"] = PrivateAttr(
        default_factory=dict
    )

    def get_subcomponent_tree(self) -> typing.Optional[nx.DiGraph]:

        if self._graph_cache is not None:
            return self._graph_cache

        childs = self.get_subcomponent_names()
        if not childs:
            return None

        graph = nx.DiGraph()

        def assemble_tree(info_model: KiaraInfoModel, current_node_id, level: int = 0):
            graph.add_node(current_node_id, obj=info_model, level=level)
            scn = info_model.get_subcomponent_names()
            if not scn:
                return
            for child_path in scn:
                child_obj = info_model.get_subcomponent(child_path)
                new_node_id = f"{current_node_id}.{child_path}"
                graph.add_edge(current_node_id, new_node_id)
                assemble_tree(child_obj, new_node_id, level + 1)

        assemble_tree(self, "__self__")

        self._graph_cache = graph
        return self._graph_cache

    def get_subcomponent_names(self) -> typing.Optional[typing.Iterable[str]]:

        if self._subcomponent_names_cache is not None:
            if self._subcomponent_names_cache is False:
                return None
            return self._subcomponent_names_cache  # type: ignore

        if self.__custom_root_type__:
            if isinstance(self.__root__, typing.Mapping):  # type: ignore
                result = set()
                for k, v in self.__root__.items():  # type: ignore
                    if isinstance(v, KiaraInfoModel):
                        result.add(k.split(".")[0])
                self._subcomponent_names_cache = sorted(result)
                return self._subcomponent_names_cache
            else:
                self._subcomponent_names_cache = False
                return None
        else:
            matches = []
            for x in self.__fields__.keys():
                _type = self.__fields__[x].type_
                if isinstance(_type, type) and issubclass(_type, KiaraInfoModel):
                    matches.append(x)
            self._subcomponent_names_cache = sorted(matches)
            return self._subcomponent_names_cache

    def get_subcomponent(self, path: str) -> "KiaraInfoModel":

        if path in self._dynamic_subcomponents.keys():
            return self._dynamic_subcomponents[path]

        if "." in path:
            first_token, rest = path.split(".", maxsplit=1)
            sc = self.get_subcomponent(first_token)
            return sc.get_subcomponent(rest)

        if self.__custom_root_type__:
            if isinstance(self.__root__, typing.Mapping):  # type: ignore
                if path in self.__root__.keys():  # type: ignore
                    return self.__root__[path]  # type: ignore
                else:
                    matches = {}
                    for k in self.__root__.keys():  # type: ignore
                        if k.startswith(f"{path}."):
                            rest = k[len(path) + 1 :]  # noqa
                            matches[rest] = self.__root__[k]  # type: ignore

                    if not matches:
                        raise KeyError(f"No child models under '{path}'.")
                    else:
                        self._dynamic_subcomponents[
                            path
                        ] = KiaraDynamicInfoModel.create_from_child_models(**matches)
                        return self._dynamic_subcomponents[path]

            else:
                raise NotImplementedError()
        else:
            if path in self.__fields__.keys():
                return getattr(self, path)
            else:
                raise KeyError(f"No child model '{path}'.")

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

    def create_html(self, **config) -> str:

        r = self.create_renderable()
        if hasattr(r, "_repr_mimebundle_"):
            mime_bundle = r._repr_mimebundle_(include=[], exclude=[])  # type: ignore
        else:
            raise NotImplementedError(
                f"Type '{self.__class__}' can't be rendered as html (yet)."
            )

        return mime_bundle["text/html"]


class KiaraDynamicInfoModel(KiaraInfoModel):

    __root__: typing.Dict[str, KiaraInfoModel]

    @classmethod
    def create_from_child_models(cls, **childs):

        return KiaraDynamicInfoModel(__root__=childs)

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE, show_lines=True)
        table.add_column("Key", style="i b")
        table.add_column("Value")
        for k, attr in self.__root__.items():
            if "documentation" in attr.__fields__.keys():
                v = attr.documentation  # type: ignore
            else:
                v = extract_renderable(attr)
            table.add_row(k, v)
        return table
