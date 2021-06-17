# -*- coding: utf-8 -*-
import os.path
import typing

from kiara import KiaraModule
from kiara.utils import dict_from_cli_args


def _create_module_instance(
    ctx, module_type: str, module_config: typing.Iterable[typing.Any]
) -> KiaraModule:
    config = dict_from_cli_args(*module_config)

    kiara_obj = ctx.obj["kiara"]
    if os.path.isfile(module_type):
        module_type = kiara_obj.register_pipeline_description(
            module_type, raise_exception=True
        )

    module_obj = kiara_obj.create_module(
        id=module_type, module_type=module_type, module_config=config
    )
    return module_obj
