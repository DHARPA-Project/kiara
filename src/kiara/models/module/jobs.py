# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import logging
import os
import uuid
from datetime import datetime
from enum import Enum
from pydantic import validator
from pydantic.fields import Field, PrivateAttr
from pydantic.main import BaseModel
from rich import box
from rich.console import RenderableType
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Union

from kiara.exceptions import InvalidValuesException
from kiara.models import KiaraModel
from kiara.models.module.manifest import InputsManifest

if TYPE_CHECKING:
    from kiara.context import DataRegistry, Kiara
    from kiara.modules import KiaraModule


class ExecutionContext(KiaraModel):

    _kiara_model_id = "instance.execution_context"

    working_dir: str = Field(
        description="The path of the working directory.", default_factory=os.getcwd
    )
    pipeline_dir: Union[str, None] = Field(
        description="The path of the pipeline file that is being executed (if applicable).",
        default=None,
    )


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

    _kiara_model_id = "instance.job_config"

    @classmethod
    def create_from_module(
        cls,
        data_registry: "DataRegistry",
        module: "KiaraModule",
        inputs: Mapping[str, Any],
    ):

        augmented = module.augment_module_inputs(inputs=inputs)

        values = data_registry.create_valuemap(
            data=augmented, schema=module.full_inputs_schema
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

    def _retrieve_data_to_hash(self) -> Any:
        return {"manifest": self.manifest_cid, "inputs": self.inputs_cid}


class ActiveJob(KiaraModel):

    _kiara_model_id = "instance.active_job"

    job_id: uuid.UUID = Field(description="The job id.")

    job_config: JobConfig = Field(description="The job details.")
    status: JobStatus = Field(
        description="The current status of the job.", default=JobStatus.CREATED
    )
    job_log: JobLog = Field(description="The lob jog.")
    submitted: datetime = Field(
        description="When the job was submitted.", default_factory=datetime.now
    )
    started: Union[datetime, None] = Field(
        description="When the job was started.", default=None
    )
    finished: Union[datetime, None] = Field(
        description="When the job was finished.", default=None
    )
    results: Union[Dict[str, uuid.UUID], None] = Field(
        description="The result(s).", default=None
    )
    error: Union[str, None] = Field(
        description="Potential error message.", default=None
    )
    _exception: Union[Exception, None] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(self.job_id)

    def _retrieve_data_to_hash(self) -> Any:
        return self.job_id.bytes

    @property
    def exception(self) -> Union[Exception, None]:
        return self._exception

    @property
    def runtime(self) -> Union[float, None]:

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

    _kiara_model_id = "instance.job_record"

    @classmethod
    def from_active_job(self, kiara: "Kiara", active_job: ActiveJob):

        assert active_job.status == JobStatus.SUCCESS
        assert active_job.results is not None

        job_details = JobRuntimeDetails.construct(
            job_log=active_job.job_log,
            submitted=active_job.submitted,
            started=active_job.started,  # type: ignore
            finished=active_job.finished,  # type: ignore
            runtime=active_job.runtime,  # type: ignore
        )

        inputs_data_cid = active_job.job_config.calculate_inputs_data_cid(
            data_registry=kiara.data_registry
        )

        job_record = JobRecord(
            job_id=active_job.job_id,
            module_type=active_job.job_config.module_type,
            module_config=active_job.job_config.module_config,
            inputs=active_job.job_config.inputs,
            outputs=active_job.results,
            runtime_details=job_details,
            environment_hashes=kiara.environment_registry.environment_hashes,
            inputs_data_hash=str(inputs_data_cid)
            if inputs_data_cid is not None
            else None,
        )
        return job_record

    job_id: uuid.UUID = Field(description="The globally unique id for this job.")
    environment_hashes: Mapping[str, Mapping[str, str]] = Field(
        description="Hashes for the environments this value was created in."
    )
    enviroments: Union[Mapping[str, Mapping[str, Any]], None] = Field(
        description="Information about the environments this value was created in.",
        default=None,
    )
    inputs_data_hash: Union[str, None] = Field(
        description="A map of the hashes of this jobs inputs."
    )

    outputs: Dict[str, uuid.UUID] = Field(description="References to the job outputs.")
    runtime_details: Union[JobRuntimeDetails, None] = Field(
        description="Runtime details for the job."
    )
    job_metadata: Mapping[str, Any] = Field(
        description="Optional metadata for this job.", default_factory=dict
    )

    _is_stored: bool = PrivateAttr(default=None)
    _outputs_hash: Union[int, None] = PrivateAttr(default=None)

    @validator("job_metadata", pre=True)
    def validate_metadata(cls, value):

        if value is None:
            value = {}
        return value

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "manifest": self.manifest_cid,
            "inputs": self.inputs_cid,
            "outputs": {k: v.bytes for k, v in self.outputs.items()},
        }

    def create_renderable(self, **config: Any) -> RenderableType:

        from kiara.utils.output import extract_renderable

        include = config.get("include", None)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for k in self.__fields__.keys():
            if include is not None and k not in include:
                continue
            attr = getattr(self, k)
            v = extract_renderable(attr)
            table.add_row(k, v)
        table.add_row("job hash", self.job_hash)
        table.add_row("inputs hash", self.inputs_hash)
        return table

    # @property
    # def outputs_hash(self) -> int:
    #
    #     if self._outputs_hash is not None:
    #         return self._outputs_hash
    #
    #     obj = self.outputs
    #     h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
    #     self._outputs_hash = h[obj]
    #     return self._outputs_hash
