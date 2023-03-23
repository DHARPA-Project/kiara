# -*- coding: utf-8 -*-
import collections.abc
from typing import TYPE_CHECKING, Dict

import orjson
from pydantic import BaseModel, Extra

from kiara.models.module.operation import Operation
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from kiara.interfaces.python_api import Workflow

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


class OperationsMap(BaseModel, collections.abc.Mapping):

    """A list of available context names."""

    class Config(object):
        json_loads = orjson.loads
        json_dumps = orjson_dumps
        extra = Extra.forbid

    __root__: Dict[str, Operation]

    def __getitem__(self, key):
        return self.__root__.__getitem__(key)

    def __iter__(self):
        return self.__root__.__iter__()

    def __len__(self):
        return self.__root__.__len__()


class WorkflowsMap(BaseModel, collections.abc.Mapping):

    """A list of available context names."""

    class Config(object):
        json_loads = orjson.loads
        json_dumps = orjson_dumps
        extra = Extra.forbid

    __root__: Dict[str, "Workflow"]

    def __getitem__(self, key):
        return self.__root__.__getitem__(key)

    def __iter__(self):
        return self.__root__.__iter__()

    def __len__(self):
        return self.__root__.__len__()
