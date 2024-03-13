# -*- coding: utf-8 -*-
from enum import Enum
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class JobCacheStrategy(Enum):

    no_cache = "no_cache"
    value_id = "value_id"
    data_hash = "data_hash"


class KiaraRuntimeConfig(BaseSettings):
    """The runtime configuration for a *kiara* backend.

    The most important option here is the 'job_cache' setting, which determines how the runtime will match a new job against the records of past ones, in order to find a matching one and not have to re-run the possibly expensive job again. By default, no matching is done, other options are matching based on exact input value ids, or (more expensive) matching based on the input data hashes.
    """

    model_config = SettingsConfigDict(
        extra="forbid", validate_assignment=True, env_prefix="kiara_runtime_"
    )

    job_cache: JobCacheStrategy = Field(
        description="Name of the strategy that determines when to re-run jobs or use cached results.",
        default=JobCacheStrategy.no_cache,
    )
    allow_external: bool = Field(
        description="Whether to allow external external pipelines.", default=True
    )
    lock_context: bool = Field(
        description="Whether to lock context(s) on creation.", default=False
    )
    runtime_profile: Literal["default", "dharpa"] = Field(
        description="The runtime profile to use, this determines for example whether comments need to be provided when running a job.",
        default="dharpa",
    )

    # ignore_errors: bool = Field(
    #     description="If set, kiara will try to ignore most errors (that can be ignored).",
    #     default=False,
    # )
