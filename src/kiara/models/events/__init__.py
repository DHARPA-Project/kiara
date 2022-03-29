# -*- coding: utf-8 -*-
import orjson
import uuid
from pydantic import BaseModel
from typing import Optional

from kiara.utils import orjson_dumps


class ChangedValue(BaseModel):

    old: Optional[uuid.UUID]
    new: Optional[uuid.UUID]


class KiaraEvent(BaseModel):
    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps
