# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from typing import Iterable, Literal, Union

from pydantic import Field

from kiara.models.events import RegistryEvent


class AliasArchiveAddedEvent(RegistryEvent):

    event_type: Literal["alias_archive_added"] = "alias_archive_added"
    alias_archive_id: uuid.UUID = Field(
        description="The unique id of this data archive."
    )
    alias_archive_alias: str = Field(
        description="The alias this data archive was added as."
    )
    is_store: bool = Field(
        description="Whether this archive supports write operations (aka implements the 'DataStore' interface)."
    )
    is_default_store: bool = Field(
        description="Whether this store acts as default store."
    )
    mount_point: Union[str, None] = Field(
        description="The mountpoint of this alias archive (if specified)."
    )


class AliasPreStoreEvent(RegistryEvent):

    event_type: Literal["alias_pre_store"] = "alias_pre_store"
    aliases: Iterable[str] = Field(description="The alias.")


class AliasStoredEvent(RegistryEvent):

    event_type: Literal["alias_stored"] = "alias_stored"
    alias: str = Field(description="The alias.")
