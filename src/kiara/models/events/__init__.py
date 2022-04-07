# -*- coding: utf-8 -*-
import orjson
import uuid
from pydantic import BaseModel
from typing import Optional

from kiara.utils import orjson_dumps, camel_case_to_snake_case


class KiaraEvent(BaseModel):

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps

    def get_event_type(self) -> str:

        if hasattr(self, "event_type"):
            return self.event_type  # type: ignore

        name = camel_case_to_snake_case(self.__class__.__name__)
        return name




