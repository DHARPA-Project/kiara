# -*- coding: utf-8 -*-
import uuid
from pydantic import Field
from typing import Literal

from kiara.models.events import RegistryEvent


class DestinyArchiveAddedEvent(RegistryEvent):

    event_type: Literal["destiny_archive_added"] = "destiny_archive_added"
    destiny_archive_id: uuid.UUID = Field(
        description="The unique id of this destiny archive."
    )
    destiny_archive_alias: str = Field(
        description="The alias this destiny archive was added as."
    )
    is_store: bool = Field(
        description="Whether this archive supports write operations (aka implements the 'DestinyStore' interface)."
    )
    is_default_store: bool = Field(
        description="Whether this store acts as default store."
    )
