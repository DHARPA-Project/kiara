# -*- coding: utf-8 -*-
import uuid
from pydantic import Field
from typing import Iterable, Literal

from kiara.models.events import KiaraEvent
from kiara.models.module.jobs import JobRecord
from kiara.models.values.value import Value


class RegistryEvent(KiaraEvent):

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context the value was created in."
    )


class DataStoreAddedEvent(RegistryEvent):

    data_archive_id: uuid.UUID = Field(description="The unique id of this data archive.")
    data_archive_alias: str = Field(description="The alias this data archive was added as.")
    is_store: bool = Field(description="Whether this archive supports write operations (aka implements the 'DataStore' interface).")
    is_default_store: bool = Field(description="Whether this store acts as default store.")

class ValueCreatedEvent(RegistryEvent):

    event_type: Literal["value_created"] = "value_created"
    value: Value = Field(description="The value metadata.")


class ValuePreStoreEvent(RegistryEvent):

    event_type: Literal["value_pre_store"] = "value_pre_store"
    value: Value = Field(description="The value metadata.")


class ValueStoredEvent(RegistryEvent):

    event_type: Literal["value_stored"] = "value_stored"
    value: Value = Field(description="The value metadata.")


class JobArchiveAddedEvent(RegistryEvent):

    job_archive_id: uuid.UUID = Field(description="The unique id of this job archive.")
    job_archive_alias: str = Field(description="The alias this job archive was added as.")
    is_store: bool = Field(description="Whether this archive supports write operations (aka implements the 'JobStore' interface).")
    is_default_store: bool = Field(description="Whether this store acts as default store.")

class JobRecordPreStoreEvent(RegistryEvent):

    event_type: Literal["value_pre_store"] = "job_record_pre_store"
    job_record: JobRecord = Field(description="The job record.")


class JobRecordStoredEvent(RegistryEvent):

    event_type: Literal["value_stored"] = "job_record_stored"
    job_record: JobRecord = Field(description="The job record.")


class AliasPreStoreEvent(RegistryEvent):

    event_type: Literal["alias_pre_store"] = "alias_pre_store"
    aliases: Iterable[str] = Field(description="The alias.")


class AliasStoredEvent(RegistryEvent):

    event_type: Literal["alias_stored"] = "alias_stored"
    alias: str = Field(description="The alias.")
