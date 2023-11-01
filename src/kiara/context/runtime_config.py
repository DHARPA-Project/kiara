# -*- coding: utf-8 -*-
from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class JobCacheStrategy(Enum):

    no_cache = "no_cache"
    value_id = "value_id"
    data_hash = "data_hash"


class KiaraRuntimeConfig(BaseSettings):
    model_config = SettingsConfigDict(
        extra="forbid", validate_assignment=True, env_prefix="kiara_runtime_"
    )

    job_cache: JobCacheStrategy = Field(
        description="Name of the strategy that determines when to re-run jobs or use cached results.",
        default=JobCacheStrategy.data_hash,
    )
    allow_external: bool = Field(
        description="Whether to allow external external pipelines.", default=True
    )
    lock_context: bool = Field(
        description="Whether to lock context(s) on creation.", default=False
    )
    # ignore_errors: bool = Field(
    #     description="If set, kiara will try to ignore most errors (that can be ignored).",
    #     default=False,
    # )
