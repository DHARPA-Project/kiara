# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from pydantic import Field
from typing import Literal

from kiara.models.events import RegistryEvent
from kiara.models.module.jobs import JobRecord


class JobArchiveAddedEvent(RegistryEvent):

    event_type: Literal["job_archive_added"] = "job_archive_added"

    job_archive_id: uuid.UUID = Field(description="The unique id of this job archive.")
    job_archive_alias: str = Field(
        description="The alias this job archive was added as."
    )
    is_store: bool = Field(
        description="Whether this archive supports write operations (aka implements the 'JobStore' interface)."
    )
    is_default_store: bool = Field(
        description="Whether this store acts as default store."
    )


class JobRecordPreStoreEvent(RegistryEvent):

    event_type: Literal["job_record_pre_store"] = "job_record_pre_store"
    job_record: JobRecord = Field(description="The job record.")


class JobRecordStoredEvent(RegistryEvent):

    event_type: Literal["job_record_stored"] = "job_record_stored"
    job_record: JobRecord = Field(description="The job record.")
