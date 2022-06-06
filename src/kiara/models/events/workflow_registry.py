# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from pydantic import Field
from typing import Literal

from kiara.models.events import RegistryEvent


class WorkflowArchiveAddedEvent(RegistryEvent):

    event_type: Literal["workflow_archive_added"] = "workflow_archive_added"
    workflow_archive_id: uuid.UUID = Field(
        description="The unique id of this data archive."
    )
    workflow_archive_alias: str = Field(
        description="The alias this workflow archive was added as."
    )
    is_store: bool = Field(
        description="Whether this archive supports write operations (aka implements the 'WorkflowStore' interface)."
    )
    is_default_store: bool = Field(
        description="Whether this store acts as default store."
    )
