# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import importlib
import typing
from functools import lru_cache
from types import ModuleType

from kiara.defaults import KIARA_MODULE_METADATA_ATTRIBUTE


@lru_cache()
def get_metadata_for_python_module_or_class(
    module_or_class: typing.Union[ModuleType, typing.Type]
) -> typing.List[typing.Dict[str, typing.Any]]:

    metadata: typing.List[typing.Dict[str, typing.Any]] = []

    if isinstance(module_or_class, type):
        if hasattr(module_or_class, KIARA_MODULE_METADATA_ATTRIBUTE):
            md = getattr(module_or_class, KIARA_MODULE_METADATA_ATTRIBUTE)
            assert isinstance(md, typing.Mapping)
            metadata.append(md)  # type: ignore
        _module_or_class: typing.Union[str, ModuleType, typing.Type] = (
            module_or_class.__module__
        )
    else:
        _module_or_class = module_or_class

    current_module = _module_or_class
    while current_module:

        if isinstance(current_module, str):
            current_module = importlib.import_module(current_module)

        if hasattr(current_module, KIARA_MODULE_METADATA_ATTRIBUTE):
            md = getattr(current_module, KIARA_MODULE_METADATA_ATTRIBUTE)
            assert isinstance(md, typing.Mapping)
            metadata.append(md)  # type: ignore

        if "." in current_module.__name__:
            current_module = ".".join(current_module.__name__.split(".")[0:-1])
        else:
            current_module = ""

    metadata.reverse()
    return metadata
