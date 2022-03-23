# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Shared utilties to be used by cli sub-commands."""


# def _create_module_instance(
#     ctx, module_type: str, module_config: typing.Iterable[typing.Any]
# ) -> KiaraModule:
#
#     kiara_obj = ctx.obj["kiara"]
#     if os.path.isfile(module_type):
#         module_type = kiara_obj.register_pipeline_description(
#             module_type, raise_exception=True
#         )
#
#     module_obj = kiara_obj.create_module(
#         id=module_type, module_type=module_type, module_config=module_config
#     )
#     return module_obj
