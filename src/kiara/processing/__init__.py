# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Protocol, Set, Union

import structlog
from pydantic import BaseModel

from kiara.exceptions import KiaraException, KiaraProcessingException
from kiara.models.module.jobs import (
    ActiveJob,
    JobConfig,
    JobLog,
    JobRecord,
    JobStatus,
)
from kiara.models.values.value import (
    ValueMap,
    ValueMapReadOnly,
    ValueMapWritable,
    ValuePedigree,
)
from kiara.modules import KiaraModule
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import get_dev_config, is_develop, log_exception
from kiara.utils.dates import get_current_time_incl_timezone

if TYPE_CHECKING:
    from kiara.context import Kiara

log = structlog.getLogger()


class JobStatusListener(Protocol):
    def job_status_changed(
        self,
        job_id: uuid.UUID,
        old_status: Union[JobStatus, None],
        new_status: JobStatus,
    ):
        pass


class ProcessorConfig(BaseModel):

    module_processor_type: Literal["synchronous", "multi-threaded"] = "synchronous"


class ModuleProcessor(abc.ABC):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._created_jobs: Dict[uuid.UUID, Dict[str, Any]] = {}
        self._running_job_details: Dict[uuid.UUID, Dict[str, Any]] = {}
        self._active_jobs: Dict[uuid.UUID, ActiveJob] = {}
        self._failed_jobs: Dict[uuid.UUID, ActiveJob] = {}
        self._finished_jobs: Dict[uuid.UUID, ActiveJob] = {}
        self._output_refs: Dict[uuid.UUID, ValueMapWritable] = {}
        self._job_records: Dict[uuid.UUID, JobRecord] = {}
        self._auto_save_jobs: Set[uuid.UUID] = set()

        self._listeners: List[JobStatusListener] = []

    def _send_job_event(
        self,
        job_id: uuid.UUID,
        old_status: Union[JobStatus, None],
        new_status: JobStatus,
    ):

        for listener in self._listeners:
            listener.job_status_changed(
                job_id=job_id, old_status=old_status, new_status=new_status
            )

    def register_job_status_listener(self, listener: JobStatusListener):

        self._listeners.append(listener)

    def get_job(self, job_id: uuid.UUID) -> ActiveJob:

        if job_id in self._active_jobs.keys():
            return self._active_jobs[job_id]
        elif job_id in self._finished_jobs.keys():
            return self._finished_jobs[job_id]
        elif job_id in self._failed_jobs.keys():
            return self._failed_jobs[job_id]
        else:
            raise Exception(f"No job with id '{job_id}' registered.")

    def get_job_status(self, job_id: uuid.UUID) -> JobStatus:

        job = self.get_job(job_id=job_id)
        return job.status

    def get_job_record(self, job_id: uuid.UUID) -> JobRecord:

        if job_id in self._job_records.keys():
            return self._job_records[job_id]
        else:
            raise Exception(f"No job record for job with id '{job_id}' registered.")

    def create_job(
        self, job_config: JobConfig, auto_save_result: bool = False
    ) -> uuid.UUID:

        environments = {
            env.model_type_id: str(env.instance_cid)
            for env in self._kiara.current_environments.values()
        }

        result_pedigree = ValuePedigree(
            kiara_id=self._kiara.id,
            module_type=job_config.module_type,
            module_config=job_config.module_config,
            inputs=job_config.inputs,
            environments=environments,
        )

        module = self._kiara.module_registry.create_module(manifest=job_config)
        unique_result_values = module.characteristics.unique_result_values

        outputs = ValueMapWritable.create_from_schema(
            kiara=self._kiara,
            schema=module.outputs_schema,
            pedigree=result_pedigree,
            unique_value_ids=unique_result_values,
        )
        job_id: uuid.UUID = ID_REGISTRY.generate(kiara_id=self._kiara.id)
        job_log = JobLog()

        job = ActiveJob(
            job_id=job_id, job_config=job_config, job_log=job_log, results=None
        )
        ID_REGISTRY.update_metadata(job_id, obj=job)
        job.job_log.add_log("job created")

        job_details: Dict[str, Any] = {
            "job_config": job_config,
            "job": job,
            "module": module,
            "outputs": outputs,
        }
        job_details["pipeline_metadata"] = job_config.pipeline_metadata

        self._created_jobs[job_id] = job_details

        self._send_job_event(
            job_id=job_id, old_status=None, new_status=JobStatus.CREATED
        )

        if is_develop():

            dev_settings = get_dev_config()

            if dev_settings.log.log_pre_run and (
                not module.characteristics.is_internal
                or dev_settings.log.pre_run.internal_modules
            ):

                is_pipeline_step = job_config.pipeline_metadata is not None
                if is_pipeline_step:
                    if dev_settings.log.pre_run.pipeline_steps:
                        step_id = job_config.pipeline_metadata.step_id  # type: ignore
                        assert step_id is not None
                        title = (
                            f"Pre-run information for pipeline step: [i]{step_id}[/i]"
                        )
                    else:
                        title = None
                else:
                    title = f"Pre-run information for module: [i]{module.module_type_name}[/i]"

                if title:
                    from kiara.utils.debug import create_module_preparation_table
                    from kiara.utils.develop import log_dev_message

                    table = create_module_preparation_table(
                        kiara=self._kiara,
                        job_config=job_config,
                        job_id=job_id,
                        module=module,
                    )
                    log_dev_message(table, title=title)

        if auto_save_result:
            self._auto_save_jobs.add(job_id)

        return job_id

    def queue_job(self, job_id: uuid.UUID) -> ActiveJob:

        job_details = self._created_jobs.pop(job_id)
        self._running_job_details[job_id] = job_details
        job_config: JobConfig = job_details.get("job_config")  # type: ignore

        job: ActiveJob = job_details.get("job")  # type: ignore
        module: KiaraModule = job_details.get("module")  # type: ignore
        outputs: ValueMapWritable = job_details.get("outputs")  # type: ignore

        self._active_jobs[job_id] = job  # type: ignore
        self._output_refs[job_id] = outputs  # type: ignore

        input_values = self._kiara.data_registry.load_values(job_config.inputs)

        if module.is_pipeline():
            module._set_job_registry(self._kiara.job_registry)  # type: ignore

        try:
            self._add_processing_task(
                job_id=job_id,
                module=module,
                inputs=input_values,
                outputs=outputs,
                job_log=job.job_log,
            )
            return job

        except Exception as e:
            msg = str(e)
            if not msg:
                msg = repr(e)
            job.error = msg

            if isinstance(e, KiaraProcessingException):
                e._module = module
                e._inputs = ValueMapReadOnly.create_from_ids(
                    data_registry=self._kiara.data_registry, **job_config.inputs
                )
                job._exception = e
                log_exception(e)
                raise e
            else:
                kpe = KiaraProcessingException(
                    e,
                    module=module,
                    inputs=ValueMapReadOnly.create_from_ids(
                        self._kiara.data_registry, **job_config.inputs
                    ),
                )
                job._exception = kpe
                log_exception(kpe)
                raise e

    def job_status_updated(
        self, job_id: uuid.UUID, status: Union[JobStatus, str, Exception]
    ):

        job = self._active_jobs.get(job_id, None)
        if job is None:
            raise Exception(
                f"Can't retrieve active job with id '{job_id}', no such job registered."
            )

        old_status = job.status

        result_values = None

        if status == JobStatus.SUCCESS:
            self._active_jobs.pop(job_id)
            job.job_log.add_log("job finished successfully")
            job.status = JobStatus.SUCCESS
            job.finished = get_current_time_incl_timezone()
            result_values = self._output_refs[job_id]
            try:
                result_values.sync_values()
                for field, val in result_values.items():
                    val.job_id = job_id

                value_ids = result_values.get_all_value_ids()
                job.results = value_ids
                job.job_log.percent_finished = 100
                job_record = JobRecord.from_active_job(
                    active_job=job, kiara=self._kiara
                )
                self._job_records[job_id] = job_record
                self._finished_jobs[job_id] = job
            except Exception as e:
                status = e
                job.job_log.add_log("job failed")
                job.status = JobStatus.FAILED
                job.finished = get_current_time_incl_timezone()
                msg = str(status)
                job.error = msg
                job._exception = status
                self._failed_jobs[job_id] = job

                log.debug(
                    "job.failed",
                    job_id=str(job.job_id),
                    msg=f"failed to sync job results: {job.error}",
                    module_type=job.job_config.module_type,
                )
                status = JobStatus.FAILED

        elif status == JobStatus.FAILED or isinstance(status, (str, Exception)):
            self._active_jobs.pop(job_id)
            job.job_log.add_log("job failed")
            job.status = JobStatus.FAILED
            job.finished = get_current_time_incl_timezone()
            if isinstance(status, str):
                job.error = status
            elif isinstance(status, Exception):
                msg = str(status)
                job.error = msg
                job._exception = status
            self._failed_jobs[job_id] = job
            log.debug(
                "job.failed",
                job_id=str(job.job_id),
                msg=job.error,
                module_type=job.job_config.module_type,
            )
            status = JobStatus.FAILED
        elif status == JobStatus.STARTED:
            job.job_log.add_log("job started")
            job.status = JobStatus.STARTED
            job.started = get_current_time_incl_timezone()
        else:
            raise ValueError(f"Invalid value for status: {status}")

        log.debug(
            "job.status_updated",
            old_status=old_status.value,
            new_status=job.status.value,
            job_id=str(job.job_id),
            module_type=job.job_config.module_type,
        )

        if status in [JobStatus.SUCCESS, JobStatus.FAILED]:
            if is_develop():
                dev_config = get_dev_config()
                if dev_config.log.log_post_run:
                    details = self._running_job_details[job_id]
                    module: KiaraModule = details["module"]
                    skip = False
                    if (
                        module.characteristics.is_internal
                        and not dev_config.log.post_run.internal_modules
                    ):
                        skip = True

                    pipeline_metadata = details.get("pipeline_metadata", None)
                    is_pipeline_step = pipeline_metadata is not None

                    if is_pipeline_step and not dev_config.log.post_run.pipeline_steps:
                        skip = True

                    if not skip:
                        if is_pipeline_step:
                            step_id = pipeline_metadata.step_id  # type: ignore
                            title = f"Post-run information for pipeline step: {step_id}"
                        else:
                            title = f"Post-run information for module: {module.module_type_name}"

                        from kiara.utils.debug import create_post_run_table
                        from kiara.utils.develop import log_dev_message

                        rendered = create_post_run_table(
                            kiara=self._kiara,
                            job=job,
                            module=module,
                            job_config=details["job_config"],
                        )
                        log_dev_message(rendered, title=title)

            self._running_job_details.pop(job_id)

        self._send_job_event(
            job_id=job_id, old_status=old_status, new_status=job.status
        )

        if status is JobStatus.SUCCESS:
            if job_id in self._auto_save_jobs:
                assert result_values is not None
                try:
                    for val in result_values.values():
                        self._kiara.data_registry.store_value(val)
                except Exception as e:
                    log_exception(e)
                    raise KiaraException(
                        msg=f"Failed to auto-save job results for job: {job_id}",
                        parent=e,
                    )

    def wait_for(self, *job_ids: uuid.UUID):
        """Wait for the jobs with the specified ids, also optionally sync their outputs with the pipeline value state."""
        self._wait_for(*job_ids)

        for job_id in job_ids:
            job = self._job_records.get(job_id, None)
            if job is None:
                _job = self._failed_jobs.get(job_id, None)
                if _job is None:
                    raise Exception(f"Can't find job with id: {job_id}")

    @abc.abstractmethod
    def _wait_for(self, *job_ids: uuid.UUID):
        pass

    @abc.abstractmethod
    def _add_processing_task(
        self,
        job_id: uuid.UUID,
        module: "KiaraModule",
        inputs: ValueMap,
        outputs: ValueMapWritable,
        job_log: JobLog,
    ) -> str:
        pass
