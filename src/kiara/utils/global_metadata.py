# -*- coding: utf-8 -*-
import importlib
import typing
from functools import lru_cache
from types import ModuleType

from kiara.defaults import KIARA_MODULE_METADATA_ATTRIBUTE
from kiara.metadata.core_models import BaseMetadataModel


@lru_cache
def get_metadata_for_python_module(module: typing.Union[str, ModuleType]):

    metadata = []
    current_module = module
    while current_module:

        if isinstance(current_module, str):
            current_module = importlib.import_module(current_module)

        print(current_module.__name__)
        if hasattr(current_module, KIARA_MODULE_METADATA_ATTRIBUTE):
            md = getattr(current_module, KIARA_MODULE_METADATA_ATTRIBUTE)
            assert isinstance(md, typing.Mapping)
            metadata.append(md)

        if "." in current_module.__name__:
            current_module = ".".join(current_module.__name__.split(".")[0:-1])
        else:
            current_module = ""

    bmm = BaseMetadataModel.from_dicts(*metadata)
    return bmm
