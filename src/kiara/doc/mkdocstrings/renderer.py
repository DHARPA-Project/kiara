# -*- coding: utf-8 -*-
#  Copyright (c) 2022-2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from collections import ChainMap
from mkdocstrings.handlers.base import BaseRenderer

from kiara.info import KiaraInfoModel


class KiaraInfoRenderer(BaseRenderer):

    default_config: dict = {}

    def render(self, data: KiaraInfoModel, config: dict) -> str:

        final_config = ChainMap(config, self.default_config)

        data_type = data.__class__.__name__.lower()
        func_name = f"render_{data_type}"
        if hasattr(self, func_name):
            func = getattr(self, func_name)

            return func(payload=data, config=final_config)
        else:
            return data.create_html()
