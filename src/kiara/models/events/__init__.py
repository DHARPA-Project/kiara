# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid

from pydantic import BaseModel, Field

from kiara.utils import camel_case_to_snake_case


class KiaraEvent(BaseModel):
    def get_event_type(self) -> str:

        if hasattr(self, "event_type"):
            return self.event_type  # type: ignore

        name = camel_case_to_snake_case(self.__class__.__name__)
        return name


class RegistryEvent(KiaraEvent):

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context the value was created in."
    )
