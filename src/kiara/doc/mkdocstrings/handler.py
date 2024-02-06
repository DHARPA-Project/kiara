# -*- coding: utf-8 -*-

#  Copyright (c) 2022-2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing

from mkdocstrings.handlers.base import BaseHandler, CollectionError, CollectorItem

__all__ = ["get_handler"]

from kiara.context import KiaraContextInfo

# from kiara.defaults import KIARA_RESOURCES_FOLDER
# from kiara.doc.mkdocstrings.collector import KiaraCollector
# from kiara.doc.mkdocstrings.renderer import KiaraInfoRenderer
from kiara.interfaces.python_api.models.info import ItemInfo
from kiara.utils import log_exception


class KiaraHandler(BaseHandler):
    """
    The kiara handler class.

    Attributes:
    ----------
        domain: The cross-documentation domain/language for this handler.
        enable_inventory: Whether this handler is interested in enabling the creation
            of the `objects.inv` Sphinx inventory file.
    """

    domain: str = "kiara"
    enable_inventory: bool = True

    def collect(
        self, identifier: str, config: typing.MutableMapping[str, typing.Any]
    ) -> CollectorItem:
        """
        Collect the documentation tree given an identifier and selection options.

        Arguments:
        ---------
            identifier: The dotted-path of a Python object available in the Python path.
            config: Selection options, used to alter the data collection done by `pytkdocs`.


        Raises:
        ------
            CollectionError: When there was a problem collecting the object documentation.


        Returns:
        -------
            The collected object-tree.
        """
        tokens = identifier.split(".")

        if tokens[0] != "kiara_info":
            return None

        item_type = tokens[1]
        item_id = ".".join(tokens[2:])
        if not item_id:
            raise CollectionError(f"Invalid id: {identifier}")

        ctx: KiaraContextInfo = builtins.plugin_package_context_info  # type: ignore  # noqa
        try:
            item: ItemInfo = ctx.get_info(item_type=item_type, item_id=item_id)
        except Exception as e:
            log_exception(e)
            raise CollectionError(f"Invalid id: {identifier}")

        return {"obj": item, "identifier": identifier}

    def get_anchors(self, data: CollectorItem) -> typing.Tuple[str, ...]:

        if data is None:
            return ()

        return (data["identifier"], data["kiara_id"], data["obj"].get_id())

    def render(
        self, data: CollectorItem, config: typing.Mapping[str, typing.Any]
    ) -> str:

        # final_config = ChainMap(config, self.default_config)

        obj = data["obj"]
        html: str = obj.create_html()
        return html


def get_handler(
    theme: str,
    custom_templates: typing.Union[str, None] = None,
    config_file_path: typing.Union[None, str] = None,
) -> KiaraHandler:
    """
    Simply return an instance of `PythonHandler`.

    Arguments:
    ---------
        theme: The theme to use when rendering contents.
        custom_templates: Directory containing custom templates.
        **config: Configuration passed to the handler.


    Returns:
    -------
        An instance of `PythonHandler`.
    """
    if custom_templates is not None:
        raise Exception("Custom templates are not supported for the kiara renderer.")

    # custom_templates = os.path.join(
    #     KIARA_RESOURCES_FOLDER, "templates", "info_templates"
    # )

    return KiaraHandler(
        "kiara",
        theme=theme,
        custom_templates=custom_templates,
    )
