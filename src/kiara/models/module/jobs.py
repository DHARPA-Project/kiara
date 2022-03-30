# -*- coding: utf-8 -*-
import logging
import uuid
from datetime import datetime
from deepdiff import DeepHash
from enum import Enum
from pydantic.fields import Field, PrivateAttr
from pydantic.main import BaseModel
from typing import Any, Dict, Mapping, Optional

from kiara.defaults import (
    JOB_CATEGORY_ID,
    JOB_CONFIG_TYPE_CATEGORY_ID,
    JOB_RECORD_TYPE_CATEGORY_ID,
    KIARA_HASH_FUNCTION,
)
from kiara.models import KiaraModel
from kiara.models.module.manifest import InputsManifest, Manifest
from kiara.models.values.value import Value


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

    log: Dict[int, LogMessage] = Field(
        description="The logs for this job.", default_factory=dict
    )
    percent_finished: int = Field(
        description="Describes how much of the job is finished. A negative number means the module does not support progress tracking.",
        default=-1,
    )

    def add_log(self, msg: str, log_level: int = logging.DEBUG):

        _msg = LogMessage(msg=msg, log_level=log_level)
        self.log[len(self.log)] = _msg


class JobConfig(InputsManifest):
    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return JOB_CONFIG_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {"module_config": self.manifest_data, "inputs": self.inputs_hash}


class JobRecord(InputsManifest):
    @classmethod
    def from_manifest(
        cls,
        manifest: Manifest,
        inputs: Mapping[str, Value],
        outputs: Mapping[str, Value],
    ):

        return JobRecord(
            module_type=manifest.module_type,
            module_config=manifest.module_config,
            inputs={k: v.value_id for k, v in inputs.items()},
            outputs={k: v.value_id for k, v in outputs.items()},
        )

    outputs: Dict[str, uuid.UUID] = Field(description="References to the job outputs.")
    _outputs_hash: Optional[int] = PrivateAttr(default=None)

    def _retrieve_category_id(self) -> str:
        return JOB_RECORD_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "manifest_hash": self.manifest_hash,
            "inputs": self.inputs,
            "outputs": self.outputs,
        }

    @property
    def outputs_hash(self) -> int:

        if self._outputs_hash is not None:
            return self._outputs_hash

        obj = self.outputs
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        self._outputs_hash = h[obj]
        return self._outputs_hash


class Job(KiaraModel):

    job_id: uuid.UUID = Field(description="The job id.", default_factory=uuid.uuid4)

    job_config: JobConfig = Field(description="The job details.")
    status: JobStatus = Field(
        description="The current status of the job.", default=JobStatus.CREATED
    )
    job_log: JobLog = Field(description="The lob jog.")
    submitted: datetime = Field(
        description="When the job was submitted.", default_factory=datetime.now
    )
    started: Optional[datetime] = Field(
        description="When the job was started.", default=None
    )
    finished: Optional[datetime] = Field(
        description="When the job was finished.", default=None
    )
    results: Optional[Dict[str, uuid.UUID]] = Field(description="The result(s).")
    error: Optional[str] = Field(description="Potential error message.")
    _exception: Optional[Exception] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(self.job_id)

    def _retrieve_category_id(self) -> str:
        return JOB_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.job_id

    @property
    def exception(self) -> Optional[Exception]:
        return self._exception

    @property
    def runtime(self) -> Optional[float]:

        if self.started is None or self.finished is None:
            return None

        runtime = self.finished - self.started
        return runtime.total_seconds()


class DeserializeConfig(JobConfig):

    output_name: str = Field(description="The name of the output field for the value.")
