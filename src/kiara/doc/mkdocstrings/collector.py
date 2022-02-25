# -*- coding: utf-8 -*-
#  Copyright (c) 2022-2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from __future__ import annotations

import networkx as nx
from mkdocstrings.handlers.base import BaseCollector, CollectorItem
from mkdocstrings.loggers import get_logger

from kiara import Kiara
from kiara.info.kiara import KiaraContext

logger = get_logger(__name__)


class KiaraCollector(BaseCollector):
    """The class responsible for loading Jinja templates and rendering them.
    It defines some configuration options, implements the `render` method,
    and overrides the `update_env` method of the [`BaseRenderer` class][mkdocstrings.handlers.base.BaseRenderer].
    """

    default_config: dict = {"docstring_style": "google", "docstring_options": {}}
    """The default selection options.
    Option | Type | Description | Default
    ------ | ---- | ----------- | -------
    **`docstring_style`** | `"google" | "numpy" | "sphinx" | None` | The docstring style to use. | `"google"`
    **`docstring_options`** | `dict[str, Any]` | The options for the docstring parser. | `{}`
    """

    fallback_config: dict = {"fallback": True}

    def __init__(self) -> None:
        """Initialize the collector."""

        self._kiara: Kiara = Kiara.instance()
        self._context: KiaraContext = KiaraContext.get_info(
            kiara=self._kiara, ignore_errors=False
        )
        self._component_tree: nx.DiGraph = self._context.get_subcomponent_tree()  # type: ignore

    def collect(self, identifier: str, config: dict) -> CollectorItem:  # noqa: WPS231
        """Collect the documentation tree given an identifier and selection options.
        Arguments:
            identifier: The dotted-path of a Python object available in the Python path.
            config: Selection options, used to alter the data collection done by `pytkdocs`.
        Raises:
            CollectionError: When there was a problem collecting the object documentation.
        Returns:
            The collected object-tree.
        """

        tokens = identifier.split(".")
        kiara_id = ".".join(tokens[1:])
        if tokens[0] != "kiara_info":
            return None
            # raise Exception(
            #     f"Handler 'kiara' can only be used with identifiers that start with 'kiara_info.', the provided id is invalid: {identifier}"
            # )
        try:
            info = self._component_tree.nodes[f"__self__.{kiara_id}"]["obj"]
        except Exception as e:
            import traceback

            traceback.print_exc()
            raise e

        return {"obj": info, "identifier": identifier, "kiara_id": kiara_id}
