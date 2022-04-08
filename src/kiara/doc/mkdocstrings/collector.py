# -*- coding: utf-8 -*-
#  Copyright (c) 2022-2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from __future__ import annotations

import builtins
from mkdocstrings.handlers.base import BaseCollector, CollectorItem
from mkdocstrings.loggers import get_logger

from kiara import Kiara

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

        if tokens[0] != "kiara_info":
            return None

        item_type = tokens[1]

        type_item_details = builtins.plugin_package_context_info
        items = type_item_details[item_type]

        item_id = ".".join(tokens[2:])

        item = items[item_id]

        return {"obj": item, "identifier": identifier}
