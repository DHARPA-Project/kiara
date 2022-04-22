# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from pydantic import Field
from typing import Literal

from kiara.models.events import RegistryEvent
from kiara.models.values.value import Value


class DataArchiveAddedEvent(RegistryEvent):

    event_type: Literal["data_archive_added"] = "data_archive_added"
    data_archive_id: uuid.UUID = Field(
        description="The unique id of this data archive."
    )
    data_archive_alias: str = Field(
        description="The alias this data archive was added as."
    )
    is_store: bool = Field(
        description="Whether this archive supports write operations (aka implements the 'DataStore' interface)."
    )
    is_default_store: bool = Field(
        description="Whether this store acts as default store."
    )


class ValueCreatedEvent(RegistryEvent):

    event_type: Literal["value_created"] = "value_created"
    value: Value = Field(description="The value metadata.")


class ValueRegisteredEvent(RegistryEvent):

    event_type: Literal["value_registered"] = "value_registered"
    value: Value = Field(description="The value metadata.")


class ValuePreStoreEvent(RegistryEvent):

    event_type: Literal["value_pre_store"] = "value_pre_store"
    value: Value = Field(description="The value metadata.")


class ValueStoredEvent(RegistryEvent):

    event_type: Literal["value_stored"] = "value_stored"
    value: Value = Field(description="The value metadata.")
