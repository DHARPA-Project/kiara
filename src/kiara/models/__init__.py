# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import networkx as nx
import orjson
import sys
from abc import ABC, abstractmethod
from deepdiff import DeepHash
from pydantic import Extra
from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel
from rich import box
from rich.console import Console, ConsoleOptions, RenderableType, RenderResult
from rich.jupyter import JupyterMixin
from rich.panel import Panel
from rich.table import Table
from typing import Any, ClassVar, Dict, Iterable, List, Optional

from kiara.defaults import KIARA_HASH_FUNCTION
from kiara.utils import orjson_dumps
from kiara.utils.models import (
    assemble_subcomponent_tree,
    get_subcomponent_from_model,
    retrieve_data_subcomponent_keys,
)


class KiaraModel(ABC, BaseModel, JupyterMixin):
    """Base class that all models in kiara inherit from.

    This class provides utility functions for things like rendering the model on terminal or as html, integration into
    a tree hierarchy of the overall kiara context, hashing, etc.
    """

    __slots__ = ["__weakref__"]

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps
        extra = Extra.forbid

        # allow_mutation = False

    @classmethod
    def get_schema_hash(cls) -> int:
        if cls._schema_hash_cache is not None:
            return cls._schema_hash_cache

        obj = cls.schema_json()
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        cls._schema_hash_cache = h[obj]
        return cls._schema_hash_cache

    _graph_cache: Optional[nx.DiGraph] = PrivateAttr(default=None)
    _subcomponent_names_cache: Optional[List[str]] = PrivateAttr(default=None)
    _dynamic_subcomponents: Dict[str, "KiaraModel"] = PrivateAttr(default_factory=dict)
    _id_cache: Optional[str] = PrivateAttr(default=None)
    _category_id_cache: Optional[str] = PrivateAttr(default=None)
    _schema_hash_cache: ClassVar = None
    _hash_cache: Optional[int] = PrivateAttr(default=None)
    _size_cache: Optional[int] = PrivateAttr(default=None)

    @abstractmethod
    def _retrieve_id(self) -> str:
        """Retrieve the unique id (with its category) of this model."""

    @abstractmethod
    def _retrieve_category_id(self) -> str:
        """Return the id of the category this model is part of."""

    @abstractmethod
    def _retrieve_data_to_hash(self) -> Any:
        """Return data important for hashing this model instance. Implemented by sub-classes.

        This returns the relevant data that makes this model unique, excluding any secondary metadata that is not
        necessary for this model to be used functionally. Like for example documentation.
        """

    @property
    def model_id(self) -> str:
        """The unique id of this model, within its category."""

        if self._id_cache is None:
            self._id_cache = self._retrieve_id()
        return self._id_cache

    @property
    def category_id(self) -> str:
        """The id of the category of this model."""
        if self._category_id_cache is None:
            self._category_id_cache = self._retrieve_category_id()
        return self._category_id_cache

    @property
    def model_data_hash(self) -> int:
        """A hash for this model."""
        if self._hash_cache is not None:
            return self._hash_cache

        obj = self._retrieve_data_to_hash()
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        self._hash_cache = h[obj]
        return self._hash_cache

    @property
    def model_size(self) -> int:

        if self._size_cache is not None:
            return self._size_cache

        self._size_cache = sys.getsizeof(self.dict())
        return self._size_cache

    # ==========================================================================================
    # subcomponent related methods
    @property
    def subcomponent_keys(self) -> Iterable[str]:
        """The keys of available sub-components of this model."""

        if self._subcomponent_names_cache is None:
            self._subcomponent_names_cache = sorted(self._retrieve_subcomponent_keys())
        return self._subcomponent_names_cache

    @property
    def subcomponent_tree(self) -> Optional[nx.DiGraph]:
        """A tree structure, containing all sub-components (and their subcomponents) of this model."""
        if not self.subcomponent_keys:
            return None

        if self._graph_cache is None:
            self._graph_cache = assemble_subcomponent_tree(self)
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
    def create_panel(self, title: str = None, **config: Any) -> Panel:

        rend = self.create_renderable(**config)
        return Panel(rend, box=box.ROUNDED, title=title, title_align="left")

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

    def create_html(self, **config) -> str:

        r = self.create_renderable()
        if hasattr(r, "_repr_mimebundle_"):
            mime_bundle = r._repr_mimebundle_(include=[], exclude=[])  # type: ignore
        else:
            raise NotImplementedError(
                f"Type '{self.__class__}' can't be rendered as html (yet)."
            )

        return mime_bundle["text/html"]

    def as_dict_with_schema(self) -> Dict[str, Dict[str, Any]]:
        return {"data": self.dict(), "schema": self.schema()}

    def as_json_with_schema(self) -> str:

        data_json = self.json()
        schema_json = self.schema_json()
        return '{"data": ' + data_json + ', "schema": ' + schema_json + "}"

    def __hash__(self):
        return self.model_data_hash

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False
        else:
            return (self.model_id, self.model_data_hash) == (
                other.model_id,
                other.model_data_hash,
            )

    def __repr__(self):

        try:
            model_id = self.model_id
        except Exception:
            model_id = "-- n/a --"

        return f"{self.__class__.__name__}(model_id={model_id}, category={self.category_id}, fields=[{', '.join(self.__fields__.keys())}])"

    def __str__(self):
        return self.__repr__()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield self.create_renderable()
