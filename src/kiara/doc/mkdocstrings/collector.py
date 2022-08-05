# -*- coding: utf-8 -*-
#  Copyright (c) 2022-2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from __future__ import annotations

import builtins
from mkdocstrings.handlers.base import BaseCollector, CollectionError, CollectorItem
from mkdocstrings.loggers import get_logger

from kiara.context import Kiara, KiaraContextInfo
from kiara.interfaces.python_api.models.info import ItemInfo
from kiara.utils import log_exception

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
        item_id = ".".join(tokens[2:])
        if not item_id:
            raise CollectionError(f"Invalid id: {identifier}")

        ctx: KiaraContextInfo = builtins.plugin_package_context_info  # type: ignore
        try:
            item: ItemInfo = ctx.get_info(item_type=item_type, item_id=item_id)
        except Exception as e:
            log_exception(e)
            raise CollectionError(f"Invalid id: {identifier}")

        return {"obj": item, "identifier": identifier}
