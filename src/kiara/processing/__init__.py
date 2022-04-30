# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import uuid
from datetime import datetime
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Protocol, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.module.jobs import ActiveJob, JobConfig, JobLog, JobRecord, JobStatus
from kiara.models.values.value import (
    ValueMap,
    ValueMapReadOnly,
    ValueMapWritable,
    ValuePedigree,
)
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import is_debug

# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


try:
    from typing import Literal

    from kiara.context import Kiara
    from kiara.modules import KiaraModule

except Exception:
    from typing_extensions import Literal  # type: ignore


class JobStatusListener(Protocol):
    def job_status_changed(
        self, job_id: uuid.UUID, old_status: Optional[JobStatus], new_status: JobStatus
    ):
        pass


class ProcessorConfig(BaseModel):

    module_processor_type: Literal["synchronous", "multi-threaded"] = "synchronous"


class ModuleProcessor(abc.ABC):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._created_jobs: Dict[uuid.UUID, Dict[str, Any]] = {}
        self._active_jobs: Dict[uuid.UUID, ActiveJob] = {}
        self._failed_jobs: Dict[uuid.UUID, ActiveJob] = {}
        self._finished_jobs: Dict[uuid.UUID, ActiveJob] = {}
        self._output_refs: Dict[uuid.UUID, ValueMapWritable] = {}
        self._job_records: Dict[uuid.UUID, JobRecord] = {}

        self._listeners: List[JobStatusListener] = []

    def _send_job_event(
        self, job_id: uuid.UUID, old_status: Optional[JobStatus], new_status: JobStatus
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

    def create_job(self, job_config: JobConfig) -> uuid.UUID:

        environments = {
            env_name: env.model_data_hash
            for env_name, env in self._kiara.current_environments.items()
        }

        result_pedigree = ValuePedigree(
            kiara_id=self._kiara.id,
            module_type=job_config.module_type,
            module_config=job_config.module_config,
            inputs=job_config.inputs,
            environments=environments,
        )

        module = self._kiara.create_module(manifest=job_config)

        outputs = ValueMapWritable.create_from_schema(
            kiara=self._kiara, schema=module.outputs_schema, pedigree=result_pedigree
        )
        job_id = ID_REGISTRY.generate(kiara_id=self._kiara.id)
        job_log = JobLog()

        job = ActiveJob.construct(job_id=job_id, job_config=job_config, job_log=job_log)
        ID_REGISTRY.update_metadata(job_id, obj=job)
        job.job_log.add_log("job created")

        job_details = {
            "job_config": job_config,
            "job": job,
            "module": module,
            "outputs": outputs,
        }
        self._created_jobs[job_id] = job_details

        self._send_job_event(
            job_id=job_id, old_status=None, new_status=JobStatus.CREATED
        )

        return job_id

    def queue_job(self, job_id: uuid.UUID) -> ActiveJob:

        job_details = self._created_jobs.pop(job_id)
        job_config = job_details.pop("job_config")

        job = job_details.pop("job")
        module = job_details.pop("module")
        outputs = job_details.pop("outputs")

        self._active_jobs[job_id] = job
        self._output_refs[job_id] = outputs

        input_values = self._kiara.data_registry.load_values(job_config.inputs)

        if module.is_pipeline():
            module._set_job_registry(self._kiara.job_registry)

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
            if is_debug():
                try:
                    import traceback

                    traceback.print_exc()
                except Exception:
                    pass
            if isinstance(e, KiaraProcessingException):
                e._module = module
                e._inputs = ValueMapReadOnly.create_from_ids(
                    data_registry=self._kiara.data_registry, **job_config.inputs
                )
                job._exception = e
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
                raise kpe

    def job_status_updated(
        self, job_id: uuid.UUID, status: Union[JobStatus, str, Exception]
    ):

        job = self._active_jobs.get(job_id, None)
        if job is None:
            raise Exception(
                f"Can't retrieve active job with id '{job_id}', no such job registered."
            )

        old_status = job.status

        if status == JobStatus.SUCCESS:
            self._active_jobs.pop(job_id)
            job.job_log.add_log("job finished successfully")
            job.status = JobStatus.SUCCESS
            job.finished = datetime.now()
            values = self._output_refs[job_id]
            values.sync_values()
            value_ids = values.get_all_value_ids()
            job.results = value_ids
            job.job_log.percent_finished = 100
            job_record = JobRecord.from_active_job(active_job=job)
            self._job_records[job_id] = job_record
            self._finished_jobs[job_id] = job
        elif status == JobStatus.FAILED or isinstance(status, (str, Exception)):
            self._active_jobs.pop(job_id)
            job.job_log.add_log("job failed")
            job.status = JobStatus.FAILED
            job.finished = datetime.now()
            if isinstance(status, str):
                job.error = status
            elif isinstance(status, Exception):
                msg = str(status)
                job.error = msg
                job._exception = status
            self._failed_jobs[job_id] = job
        elif status == JobStatus.STARTED:
            job.job_log.add_log("job started")
            job.status = JobStatus.STARTED
            job.started = datetime.now()
        else:
            raise ValueError(f"Invalid value for status: {status}")

        self._send_job_event(
            job_id=job_id, old_status=old_status, new_status=job.status
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
