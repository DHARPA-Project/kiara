# -*- coding: utf-8 -*-
import logging
import typing
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, validator

from kiara.pipeline import PipelineValues

try:
    from typing import Literal
except Exception:
    from typing_extensions import Literal  # type: ignore  # noqa


class JobStatus(Enum):

    CREATED = "__job_created__"
    STARTED = "__job_started__"
    SUCCESS = "__job_success__"
    FAILED = "__job_failed__"


class LogMessage(BaseModel):

    timestamp: datetime = Field(
        description="The time the message was logged.", default_factory=datetime.now
    )
    log_level: int = Field(description="The log level.")
    msg: str = Field(description="The log message")


class JobLog(BaseModel):

    log: typing.Dict[int, LogMessage] = Field(
        description="The logs for this job.", default_factory=dict
    )
    percent_finished: int = Field(
        description="Describes how much of the job is finished. A negative number means the module does not support progress tracking.",
        default=-1,
    )

    def add_log(self, msg: str, log_level: int = logging.DEBUG):

        _msg = LogMessage(msg=msg, log_level=log_level)
        self.log[len(self.log)] = _msg


class Job(BaseModel):
    @classmethod
    def create_event_msg(cls, job: "Job"):

        topic = job.status.value[2:-2]

        payload = f"{topic} {job.json()}"
        return payload

    class Config:
        use_enum_values = True

    id: str = Field(description="The id of the job.")
    pipeline_id: str = Field(description="The id of the pipeline this jobs runs for.")
    pipeline_name: str = Field(description="The name/type of the pipeline.")
    step_id: str = Field(description="The id of the step within the pipeline.")
    module_type: str = Field(description="The module type name.")
    module_config: typing.Dict[str, typing.Any] = Field(
        description="The module configuration."
    )
    inputs: PipelineValues = Field(description="The input values.")
    outputs: PipelineValues = Field(description="The output values.")
    job_log: JobLog = Field(
        description="Details about the job progress.", default_factory=JobLog
    )
    submitted: datetime = Field(
        description="When the job was submitted.", default_factory=datetime.now
    )
    started: typing.Optional[datetime] = Field(
        description="When the job was started.", default=None
    )
    finished: typing.Optional[datetime] = Field(
        description="When the job was finished.", default=None
    )
    status: JobStatus = Field(
        description="The current status of the job.",
        default=JobStatus.CREATED,
    )
    error: typing.Optional[str] = Field(description="Potential error message.")

    @validator("status")
    def _validate_status(cls, v):

        if isinstance(v, int):
            if v < 0 or v > 100:
                raise ValueError(
                    "Status must be a status string, or an integer between 0 and 100."
                )

        return v
