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
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Mapping, Union

from pydantic import field_validator
from pydantic.fields import Field, PrivateAttr
from pydantic.main import BaseModel
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.exceptions import InvalidValuesException, KiaraException
from kiara.models import KiaraModel
from kiara.models.module.manifest import InputsManifest
from kiara.utils.dates import get_current_time_incl_timezone

if TYPE_CHECKING:
    from kiara.context import DataRegistry, Kiara
    from kiara.modules import KiaraModule


class ExecutionContext(KiaraModel):

    _kiara_model_id: ClassVar = "instance.execution_context"

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


class PipelineMetadata(BaseModel):

    pipeline_id: uuid.UUID = Field(description="The id of the pipeline.")
    step_id: str = Field(description="The id of the step in the pipeline.")


class JobConfig(InputsManifest):

    _kiara_model_id: ClassVar = "instance.job_config"

    @classmethod
    def create_from_module(
        cls,
        data_registry: "DataRegistry",
        module: "KiaraModule",
        inputs: Mapping[str, Any],
    ) -> "JobConfig":

        augmented = module.augment_module_inputs(inputs=inputs)

        values = data_registry.create_valuemap(
            data=augmented, schema=module.full_inputs_schema
        )

        invalid = values.check_invalid()
        if invalid:
            raise InvalidValuesException(invalid_values=invalid)

        value_ids = values.get_all_value_ids()

        if not module.manifest.is_resolved:
            raise KiaraException(
                msg="Cannot create job config from unresolved manifest."
            )

        return JobConfig(
            module_type=module.manifest.module_type,
            module_config=module.manifest.module_config,
            is_resolved=module.manifest.is_resolved,
            inputs=value_ids,
        )

    def _retrieve_data_to_hash(self) -> Any:
        return {"manifest": self.manifest_cid, "inputs": self.inputs_cid}

    pipeline_metadata: Union[PipelineMetadata, None] = Field(
        description="Metadata for the pipeline this job is part of.", default=None
    )
    # job_metadata: Mapping[str, Any] = Field(
    #     description="Optional metadata for this job.", default_factory=dict
    # )


class ActiveJob(KiaraModel):

    _kiara_model_id: ClassVar = "instance.active_job"

    job_id: uuid.UUID = Field(description="The job id.")

    job_config: JobConfig = Field(description="The job details.")
    status: JobStatus = Field(
        description="The current status of the job.", default=JobStatus.CREATED
    )
    job_log: JobLog = Field(description="The lob jog.")
    submitted: datetime = Field(
        description="When the job was submitted.",
        default_factory=get_current_time_incl_timezone,
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

    def is_finished(self) -> bool:
        return self.finished is not None

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
    runtime: float = Field(description="The duration of the job (in seconds).")

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        table.add_row("submitted", str(self.submitted))
        table.add_row("started", str(self.started))
        table.add_row("finished", str(self.finished))
        table.add_row("runtime", f"{self.runtime} seconds")

        job_log_table = Table(show_header=False, box=box.SIMPLE)
        job_log_table.add_column("timestamp", style="i")
        job_log_table.add_column("message")
        for log in self.job_log.log:
            job_log_table.add_row(str(log.timestamp), log.msg)

        table.add_row("job log", job_log_table)

        return table


class JobRecord(JobConfig):

    _kiara_model_id: ClassVar = "instance.job_record"

    @classmethod
    def from_active_job(self, kiara: "Kiara", active_job: ActiveJob):

        assert active_job.status == JobStatus.SUCCESS
        assert active_job.results is not None

        job_details = JobRuntimeDetails(
            job_log=active_job.job_log,
            submitted=active_job.submitted,
            started=active_job.started,  # type: ignore
            finished=active_job.finished,  # type: ignore
            runtime=active_job.runtime,  # type: ignore
        )

        (
            inputs_data_cid,
            contains_invalid,
        ) = active_job.job_config.calculate_inputs_data_cid(
            data_registry=kiara.data_registry
        )
        inputs_data_hash = str(inputs_data_cid)

        module = kiara.module_registry.create_module(active_job.job_config)
        is_internal = module.characteristics.is_internal

        env_hashes = {
            env.model_type_id: str(env.instance_cid)
            for env in kiara.current_environments.values()
        }

        job_record = JobRecord(
            job_id=active_job.job_id,
            job_submitted=active_job.submitted,
            is_internal=is_internal,
            module_type=active_job.job_config.module_type,
            module_config=active_job.job_config.module_config,
            is_resolved=active_job.job_config.is_resolved,
            inputs=active_job.job_config.inputs,
            outputs=active_job.results,
            runtime_details=job_details,
            environment_hashes=env_hashes,
            # input_ids_hash=active_job.job_config.input_ids_hash,
            inputs_data_hash=inputs_data_hash,
        )
        job_record._manifest_cid = active_job.job_config.manifest_cid
        job_record._manifest_data = active_job.job_config.manifest_data
        job_record._jobs_cid = active_job.job_config.job_cid
        job_record._inputs_cid = active_job.job_config.inputs_cid
        return job_record

    job_id: uuid.UUID = Field(description="The globally unique id for this job.")
    job_submitted: datetime = Field(description="When the job was submitted.")
    environment_hashes: Mapping[str, str] = Field(
        description="Hashes for the environments this value was created in."
    )
    # enviroments: Union[Mapping[str, Mapping[str, Any]], None] = Field(
    #     description="Information about the environments this value was created in.",
    #     default=None,
    # )
    is_internal: bool = Field(description="Whether this job was created by the system.")
    # job_hash: str = Field(description="The hash of the job. Calculated from manifest & input_ids hashes.")
    # manifest_hash: str = Field(description="The hash of the manifest.")
    # input_ids_hash: str = Field(description="The hash of the field names and input ids (the value_ids/uuids).")
    inputs_data_hash: str = Field(
        description="A map of the hashes of this jobs inputs (the hashes of field names and the actual bytes)."
    )

    outputs: Dict[str, uuid.UUID] = Field(description="References to the job outputs.")
    runtime_details: Union[JobRuntimeDetails, None] = Field(
        description="Runtime details for the job."
    )
    # job_metadata: Mapping[str, Any] = Field(
    #     description="Optional metadata for this job.", default_factory=dict
    # )

    _is_stored: bool = PrivateAttr(default=None)
    _outputs_hash: Union[int, None] = PrivateAttr(default=None)

    # @field_validator("job_metadata", mode="before")
    # @classmethod
    # def validate_metadata(cls, value):
    #
    #     if value is None:
    #         value = {}
    #     return value

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
        for k in self.model_fields.keys():
            if include is not None and k not in include:
                continue
            attr = getattr(self, k)
            v = extract_renderable(attr)
            table.add_row(k, v)
        table.add_row("job hash", self.job_hash)
        table.add_row("inputs hash", self.input_ids_hash)
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


class JobMatcher(KiaraModel):
    @classmethod
    def create_matcher(self, **match_options: Any):

        m = JobMatcher(**match_options)
        return m

    job_ids: List[uuid.UUID] = Field(
        description="A list of job ids, if specified, only jobs with one of these ids will be included.",
        default_factory=list,
    )
    allow_internal: bool = Field(description="Allow internal jobs.", default=False)
    earliest: Union[None, datetime] = Field(
        description="The earliest time when the job was created.", default=None
    )
    latest: Union[None, datetime] = Field(
        description="The latest time when the job was created.", default=None
    )
    operation_inputs: List[uuid.UUID] = Field(
        description="A list of value ids, if specified, only jobs that use one of them will be included.",
        default_factory=list,
    )
    produced_outputs: List[uuid.UUID] = Field(
        description="A list of value ids, if specified, only jobs that produced one of them will be included.",
        default_factory=list,
    )

    @field_validator("job_ids", mode="before")
    @classmethod
    def validate_job_ids(cls, v):

        if v is None:
            return []
        elif isinstance(v, uuid.UUID):
            return [v]
        elif isinstance(v, str):
            return [uuid.UUID(v)]
        else:
            return [x if isinstance(x, uuid.UUID) else uuid.UUID(x) for x in v]
