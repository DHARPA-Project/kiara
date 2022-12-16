# -*- coding: utf-8 -*-

#  Copyright (c) 2022-2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from mkdocstrings.handlers.base import BaseHandler

__all__ = ["get_handler"]  # noqa: WPS410


# from kiara.defaults import KIARA_RESOURCES_FOLDER
from kiara.doc.mkdocstrings.collector import KiaraCollector
from kiara.doc.mkdocstrings.renderer import KiaraInfoRenderer


class KiaraHandler(BaseHandler):
    """The kiara handler class.
    Attributes:
        domain: The cross-documentation domain/language for this handler.
        enable_inventory: Whether this handler is interested in enabling the creation
            of the `objects.inv` Sphinx inventory file.
    """

    domain: str = "kiara"
    enable_inventory: bool = True

    # load_inventory = staticmethod(inventory.list_object_urls)
    #
    # @classmethod
    # def load_inventory(
    #     cls,
    #     in_file: typing.BinaryIO,
    #     url: str,
    #     base_url: typing.Optional[str] = None,
    #     **kwargs: typing.Any,
    # ) -> typing.Iterator[typing.Tuple[str, str]]:
    #     """Yield items and their URLs from an inventory file streamed from `in_file`.
    #     This implements mkdocstrings' `load_inventory` "protocol" (see plugin.py).
    #     Arguments:
    #         in_file: The binary file-like object to read the inventory from.
    #         url: The URL that this file is being streamed from (used to guess `base_url`).
    #         base_url: The URL that this inventory's sub-paths are relative to.
    #         **kwargs: Ignore additional arguments passed from the config.
    #     Yields:
    #         Tuples of (item identifier, item URL).
    #     """
    #
    #     print("XXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    #
    #     if base_url is None:
    #         base_url = posixpath.dirname(url)
    #
    #     for item in Inventory.parse_sphinx(
    #         in_file, domain_filter=("py",)
    #     ).values():  # noqa: WPS526
    #         yield item.name, posixpath.join(base_url, item.uri)


def get_handler(
    theme: str,  # noqa: W0613 (unused argument config)
    custom_templates: typing.Union[str, None] = None,
    **config: typing.Any,
) -> KiaraHandler:
    """Simply return an instance of `PythonHandler`.
    Arguments:
        theme: The theme to use when rendering contents.
        custom_templates: Directory containing custom templates.
        **config: Configuration passed to the handler.
    Returns:
        An instance of `PythonHandler`.
    """

    if custom_templates is not None:
        raise Exception("Custom templates are not supported for the kiara renderer.")

    # custom_templates = os.path.join(
    #     KIARA_RESOURCES_FOLDER, "templates", "info_templates"
    # )

    return KiaraHandler(
        collector=KiaraCollector(),
        # renderer=KiaraInfoRenderer("kiara", theme, custom_templates),
        renderer=KiaraInfoRenderer("kiara", theme),
    )
