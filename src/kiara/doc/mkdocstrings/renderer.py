# -*- coding: utf-8 -*-

#  Copyright (c) 2022-2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from mkdocstrings.handlers.base import BaseRenderer, CollectorItem


class AliasResolutionError:
    pass


class KiaraInfoRenderer(BaseRenderer):

    default_config: dict = {}

    def get_anchors(
        self, data: CollectorItem
    ) -> typing.List[str]:  # noqa: D102 (ignore missing docstring)

        if data is None:
            return list()

        return list([data["identifier"], data["kiara_id"], data["obj"].get_id()])

    def render(self, data: typing.Dict[str, typing.Any], config: dict) -> str:

        # final_config = ChainMap(config, self.default_config)

        obj = data["obj"]
        html = obj.create_html()
        return html
