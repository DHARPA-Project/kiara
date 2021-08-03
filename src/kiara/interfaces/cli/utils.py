# -*- coding: utf-8 -*-
"""Shared utilties to be used by cli sub-commands."""

import os.path
import typing

from kiara import KiaraModule


def _create_module_instance(
    ctx, module_type: str, module_config: typing.Iterable[typing.Any]
) -> KiaraModule:

    kiara_obj = ctx.obj["kiara"]
    if os.path.isfile(module_type):
        module_type = kiara_obj.register_pipeline_description(
            module_type, raise_exception=True
        )

    module_obj = kiara_obj.create_module(
        id=module_type, module_type=module_type, module_config=module_config
    )
    return module_obj
