# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from enum import Enum
from pydantic import BaseModel, Field


class ValueStatus(Enum):

    UNKNONW = "unknown"
    NOT_SET = "not set"
    NONE = "none"
    DEFAULT = "default"
    SET = "set"


class DataTypeCharacteristics(BaseModel):

    is_scalar: bool = Field(
        description="Whether the data desribed by this data type behaves like a skalar.",
        default=False,
    )
    is_json_serializable: bool = Field(
        description="Whether the data can be serialized to json without information loss.",
        default=False,
    )


DEFAULT_SCALAR_DATATYPE_CHARACTERISTICS = DataTypeCharacteristics.construct(
    is_scalar=True, is_json_serializable=True
)
