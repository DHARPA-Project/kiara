# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import logging
import uuid
from datetime import datetime
from deepdiff import DeepHash
from enum import Enum
from pydantic.fields import Field, PrivateAttr
from pydantic.main import BaseModel
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

from kiara.defaults import (
    JOB_CATEGORY_ID,
    JOB_CONFIG_TYPE_CATEGORY_ID,
    JOB_RECORD_TYPE_CATEGORY_ID,
    KIARA_HASH_FUNCTION,
)
from kiara.exceptions import InvalidValuesException
from kiara.models import KiaraModel
from kiara.models.module.manifest import InputsManifest

if TYPE_CHECKING:
    from kiara.context import DataRegistry
    from kiara.modules import KiaraModule


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

    log: List[LogMessage] = Field(
        description="The logs for this job.", default_factory=list
    )
    percent_finished: int = Field(
        description="Describes how much of the job is finished. A negative number means the module does not support progress tracking.",
        default=-1,
    )

    def add_log(self, msg: str, log_level: int = logging.DEBUG):

        _msg = LogMessage(msg=msg, log_level=log_level)
        self.log.append(_msg)


class JobConfig(InputsManifest):
    @classmethod
    def create_from_module(
        cls,
        data_registry: "DataRegistry",
        module: "KiaraModule",
        inputs: Mapping[str, Any],
    ):

        augmented = module.augment_module_inputs(inputs=inputs)
        values = data_registry.create_valueset(
            data=augmented, schema=module.inputs_schema
        )

        invalid = values.check_invalid()
        if invalid:
            raise InvalidValuesException(invalid_values=invalid)

        value_ids = values.get_all_value_ids()
        return JobConfig.construct(
            module_type=module.module_type_name,
            module_config=module.config.dict(),
            inputs=value_ids,
        )

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return JOB_CONFIG_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {"manifest": self.manifest_data, "inputs": self.inputs_hash}


class ActiveJob(KiaraModel):

    job_id: uuid.UUID = Field(description="The job id.")

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


class JobRuntimeDetails(BaseModel):

    # @classmethod
    # def from_manifest(
    #     cls,
    #     manifest: Manifest,
    #     inputs: Mapping[str, Value],
    #     outputs: Mapping[str, Value],
    # ):
    #
    #     return JobRecord(
    #         module_type=manifest.module_type,
    #         module_config=manifest.module_config,
    #         inputs={k: v.value_id for k, v in inputs.items()},
    #         outputs={k: v.value_id for k, v in outputs.items()},
    #     )

    job_log: JobLog = Field(description="The lob jog.")
    submitted: datetime = Field(description="When the job was submitted.")
    started: datetime = Field(description="When the job was started.")
    finished: datetime = Field(description="When the job was finished.")
    runtime: float = Field(description="The duration of the job.")


class JobRecord(JobConfig):
    @classmethod
    def from_active_job(self, active_job: ActiveJob):

        assert active_job.status == JobStatus.SUCCESS
        assert active_job.results is not None

        job_details = JobRuntimeDetails.construct(
            job_log=active_job.job_log,
            submitted=active_job.submitted,
            started=active_job.started,  # type: ignore
            finished=active_job.finished,  # type: ignore
            runtime=active_job.runtime,  # type: ignore
        )

        job_record = JobRecord.construct(
            job_id=active_job.job_id,
            module_type=active_job.job_config.module_type,
            module_config=active_job.job_config.module_config,
            inputs=active_job.job_config.inputs,
            outputs=active_job.results,
            runtime_details=job_details,
        )
        return job_record

    job_id: uuid.UUID = Field(description="The globally unique id for this job.")
    outputs: Dict[str, uuid.UUID] = Field(description="References to the job outputs.")
    runtime_details: Optional[JobRuntimeDetails] = Field(
        description="Runtime details for the job."
    )

    _is_stored: bool = PrivateAttr(default=None)
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
