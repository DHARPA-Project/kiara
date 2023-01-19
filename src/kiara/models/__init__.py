# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import networkx as nx
import orjson
from abc import ABC
from dag_cbor.encoding import EncodableType
from deepdiff import DeepHash
from multiformats import CID
from pydantic import Extra
from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel
from rich import box
from rich.console import Console, ConsoleOptions, RenderableType, RenderResult
from rich.jupyter import JupyterMixin
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from typing import Any, ClassVar, Dict, Iterable, List, Mapping, Union

from kiara.defaults import KIARA_HASH_FUNCTION
from kiara.registries.templates import TemplateRegistry
from kiara.utils.class_loading import _default_id_func
from kiara.utils.develop import log_dev_message
from kiara.utils.hashing import compute_cid
from kiara.utils.json import orjson_dumps
from kiara.utils.models import (
    assemble_subcomponent_graph,
    create_subcomponent_tree_renderable,
    get_subcomponent_from_model,
    retrieve_data_subcomponent_keys,
)


class KiaraModel(ABC, BaseModel, JupyterMixin):
    """Base class that all models in kiara inherit from.

    This class provides utility functions for things like rendering the model on terminal or as html, integration into
    a tree hierarchy of the overall kiara context, hashing, etc.
    """

    __slots__ = ["__weakref__"]

    class Config(object):
        json_loads = orjson.loads
        json_dumps = orjson_dumps
        extra = Extra.forbid

    # @classmethod
    # def get_model_title(cls):
    #
    #     return to_camel_case(cls._kiara_model_name)

    @classmethod
    def get_schema_hash(cls) -> int:
        if cls._schema_hash_cache is not None:
            return cls._schema_hash_cache

        obj = cls.schema_json()
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        cls._schema_hash_cache = h[obj]
        return cls._schema_hash_cache

    _graph_cache: Union[nx.DiGraph, None] = PrivateAttr(default=None)
    _subcomponent_names_cache: Union[List[str], None] = PrivateAttr(default=None)
    _dynamic_subcomponents: Dict[str, "KiaraModel"] = PrivateAttr(default_factory=dict)
    _id_cache: Union[str, None] = PrivateAttr(default=None)
    _category_id_cache: Union[str, None] = PrivateAttr(default=None)
    _schema_hash_cache: ClassVar = None
    _cid_cache: Union[CID, None] = PrivateAttr(default=None)
    _dag_cache: Union[bytes, None] = PrivateAttr(default=None)
    _size_cache: Union[int, None] = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> EncodableType:
        """Return data important for hashing this model instance. Implemented by sub-classes.

        This returns the relevant data that makes this model unique, excluding any secondary metadata that is not
        necessary for this model to be used functionally. Like for example documentation.
        """

        return self.dict()

    @property
    def instance_id(self) -> str:
        """The unique id of this model, within its category."""

        if self._id_cache is not None:
            return self._id_cache

        self._id_cache = self._retrieve_id()
        return self._id_cache

    @property
    def instance_cid(self) -> CID:
        if self._cid_cache is None:
            self._compute_cid()
        return self._cid_cache  # type: ignore

    @property
    def instance_dag(self) -> bytes:

        if self._dag_cache is None:
            self._compute_cid()
        return self._dag_cache  # type: ignore

    @property
    def instance_size(self) -> int:

        if self._size_cache is None:
            self._compute_cid()
        return self._size_cache  # type: ignore

    @property
    def model_type_id(self) -> str:
        """The id of the category of this model."""

        if hasattr(self.__class__, "_kiara_model_id"):
            return self._kiara_model_id  # type: ignore
        else:
            return _default_id_func(self.__class__)

    def _retrieve_id(self) -> str:
        return str(self.instance_cid)

    def _compute_cid(self):
        """A hash for this model."""
        if self._cid_cache is not None:
            return

        obj = self._retrieve_data_to_hash()
        dag, cid = compute_cid(data=obj)

        self._cid_cache = cid
        self._dag_cache = dag
        self._size_cache = len(dag)

    # ==========================================================================================
    # subcomponent related methods
    @property
    def subcomponent_keys(self) -> Iterable[str]:
        """The keys of available sub-components of this model."""

        if self._subcomponent_names_cache is None:
            self._subcomponent_names_cache = sorted(self._retrieve_subcomponent_keys())
        return self._subcomponent_names_cache

    @property
    def subcomponent_tree(self) -> Union[nx.DiGraph, None]:
        """A tree structure, containing all sub-components (and their subcomponents) of this model."""
        if not self.subcomponent_keys:
            return None

        if self._graph_cache is None:
            self._graph_cache = assemble_subcomponent_graph(self)
        return self._graph_cache

    def get_subcomponent(self, path: str) -> "KiaraModel":
        """Retrieve the subcomponent identified by the specified path."""

        if path not in self._dynamic_subcomponents.keys():
            self._dynamic_subcomponents[path] = self._retrieve_subcomponent(path=path)
        return self._dynamic_subcomponents[path]

    def find_subcomponents(self, category: str) -> Dict[str, "KiaraModel"]:
        """Find and return all subcomponents of this model that are member of the specified category."""
        tree = self.subcomponent_tree
        if tree is None:
            raise Exception(f"No subcomponents found for category: {category}")

        result = {}
        for node_id, node in tree.nodes.items():
            if not hasattr(node["obj"], "get_category_alias"):
                raise NotImplementedError()

            if category != node["obj"].get_category_alias():
                continue

            n_id = node_id[9:]  # remove the __self__. token
            result[n_id] = node["obj"]
        return result

    def _retrieve_subcomponent_keys(self) -> Iterable[str]:
        """Retrieve the keys of all subcomponents of this model.

        Can be overwritten in sub-classes, by default it tries to automatically determine the subcomponents.
        """

        return retrieve_data_subcomponent_keys(self)

    def _retrieve_subcomponent(self, path: str) -> "KiaraModel":
        """Retrieve the subcomponent under the specified path.

        Can be overwritten in sub-classes, by default it tries to automatically determine the subcomponents.
        """

        m = get_subcomponent_from_model(self, path=path)
        return m

    # ==========================================================================================
    # model rendering related methods
    def create_panel(self, title: Union[str, None] = None, **config: Any) -> Panel:

        rend = self.create_renderable(**config)
        return Panel(rend, box=box.ROUNDED, title=title, title_align="left")

    def create_html(self, **config) -> str:

        template_registry = TemplateRegistry.instance()
        template = template_registry.get_template_for_model_type(
            model_type=self.model_type_id, template_format="html"
        )

        if template:
            try:
                result = template.render(instance=self)
                return result
            except Exception as e:
                log_dev_message(
                    title="html-rendering error",
                    msg=f"Failed to render html for model '{self.instance_id}' type '{self.model_type_id}': {e}",
                )

        try:
            from kiara.utils.html import generate_html

            html = generate_html(item=self, add_header=False)
            return html
        except Exception as e:
            log_dev_message(
                title="html-generation error",
                msg=f"Failed to generate html for model '{self.instance_id}' type '{self.model_type_id}': {e}",
            )

        r = self.create_renderable(**config)
        mime_bundle = r._repr_mimebundle_(include=[], exclude=[])  # type: ignore
        return mime_bundle["text/html"]

    def create_renderable(self, **config: Any) -> RenderableType:

        from kiara.utils.output import extract_renderable

        include = config.get("include", None)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for k in self.__fields__.keys():
            if include is not None and k not in include:
                continue
            attr = getattr(self, k)
            v = extract_renderable(attr)
            table.add_row(k, v)
        return table

    def create_renderable_tree(self, **config: Any) -> Tree:

        show_data = config.get("show_data", False)
        tree = create_subcomponent_tree_renderable(data=self, show_data=show_data)
        return tree

    def create_info_data(self, **config) -> Mapping[str, Any]:

        include = config.get("include", None)

        result = {}
        for k in self.__fields__.keys():
            if include is not None and k not in include:
                continue
            attr = getattr(self, k)
            v = attr
            result[k] = v
        return result

    def as_dict_with_schema(self) -> Dict[str, Dict[str, Any]]:
        return {"data": self.dict(), "schema": self.schema()}

    def as_json_with_schema(self) -> str:

        data_json = self.json()
        schema_json = self.schema_json()
        return '{"data": ' + data_json + ', "schema": ' + schema_json + "}"

    def __hash__(self):
        return int.from_bytes(self.instance_cid.digest, "big")

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False
        else:
            return (self.instance_id, self.instance_cid) == (
                other.instance_id,
                other.instance_cid,
            )

    def __repr__(self):

        try:
            model_id = self.instance_id
        except Exception:
            model_id = "-- n/a --"

        return f"{self.__class__.__name__}(model_id={model_id}, category={self.model_type_id}, fields=[{', '.join(self.__fields__.keys())}])"

    def __str__(self):
        return self.__repr__()

    def _repr_html_(self):
        return str(self.create_html())

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield self.create_renderable()

    # def _repr_mimebundle_(
    #     self: "ConsoleRenderable",
    #     include: Sequence[str],
    #     exclude: Sequence[str],
    #     **kwargs: Any,
    # ) -> Dict[str, str]:
    #
    #     console = get_console()
    #     segments = list(console.render(self, console.options))
    #     html = _render_segments(segments)
    #     text = console._render_buffer(segments)
    #     data = {"text/plain": text, "text/html": html}
    #     if include:
    #         data = {k: v for (k, v) in data.items() if k in include}
    #     if exclude:
    #         data = {k: v for (k, v) in data.items() if k not in exclude}
    #     return data
