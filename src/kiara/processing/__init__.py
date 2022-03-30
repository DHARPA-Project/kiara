# -*- coding: utf-8 -*-
import abc
import uuid
from datetime import datetime
from pydantic import BaseModel
from typing import Dict, Mapping, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.module.jobs import Job, JobConfig, JobLog, JobStatus
from kiara.models.values.value import (
    ValuePedigree,
    ValueSetReadOnly,
    ValueSetWritable,
)
from kiara.utils import is_debug

# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


try:
    from typing import Literal

    from kiara.kiara import Kiara
    from kiara.modules import KiaraModule

except Exception:
    from typing_extensions import Literal  # type: ignore


class ProcessorConfig(BaseModel):

    module_processor_type: Literal["synchronous", "multi-threaded"] = "synchronous"


class ModuleProcessor(abc.ABC):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._active_jobs: Dict[uuid.UUID, Job] = {}
        self._output_refs: Dict[uuid.UUID, ValueSetWritable] = {}
        self._finished_jobs: Dict[uuid.UUID, Job] = {}

    def get_job(self, job_id: uuid.UUID) -> Job:

        if job_id in self._active_jobs.keys():
            return self._active_jobs[job_id]
        elif job_id in self._finished_jobs.keys():
            return self._finished_jobs[job_id]
        else:
            raise Exception(f"No job with id '{job_id}' registered.")

    def process_job(self, job_config: JobConfig) -> uuid.UUID:

        environments = {
            env_name: env.model_data_hash
            for env_name, env in self._kiara.environments.items()
        }

        result_pedigree = ValuePedigree(
            kiara_id=self._kiara.id,
            module_type=job_config.module_type,
            module_config=job_config.module_config,
            inputs=job_config.inputs,
            environments=environments,
        )

        module = self._kiara.create_module(manifest=job_config)

        outputs = ValueSetWritable.create_from_schema(
            kiara=self._kiara, schema=module.outputs_schema, pedigree=result_pedigree
        )
        job_id = uuid.uuid4()
        job_log = JobLog()

        job = Job.construct(job_id=job_id, job_config=job_config, job_log=job_log)

        job.job_log.add_log("job created")
        self._active_jobs[job_id] = job
        self._output_refs[job_id] = outputs

        try:
            self.queue_job(
                job_id=job_id,
                module=module,
                inputs=job_config.inputs,
                outputs=outputs,
                job_log=job.job_log,
            )

            return job_id
        except Exception as e:
            job.error = str(e)
            if is_debug():
                try:
                    import traceback

                    traceback.print_exc()
                except Exception:
                    pass
            if isinstance(e, KiaraProcessingException):
                e._module = module
                e._inputs = ValueSetReadOnly.create_from_ids(
                    data_registry=self._kiara.data_registry, **job_config.inputs
                )
                job._exception = e
                raise e
            else:
                kpe = KiaraProcessingException(
                    e,
                    module=module,
                    inputs=ValueSetReadOnly.create_from_ids(
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

        if status == JobStatus.SUCCESS:
            self._active_jobs.pop(job_id)
            job.job_log.add_log("job finished successfully")
            job.status = JobStatus.SUCCESS
            job.finished = datetime.now()
            values = self._output_refs[job_id]
            values.sync_values()
            value_ids = values.get_all_value_ids()
            job.results = value_ids
            self._finished_jobs[job_id] = job
        elif status == JobStatus.FAILED or isinstance(status, (str, Exception)):
            self._active_jobs.pop(job_id)
            job.job_log.add_log("job failed")
            job.status = JobStatus.FAILED
            job.finished = datetime.now()
            if isinstance(status, str):
                job.error = status
            elif isinstance(status, Exception):
                job.error = str(status)
                job._exception = status
            self._finished_jobs[job_id] = job
        elif status == JobStatus.STARTED:
            job.job_log.add_log("job started")
            job.status = JobStatus.STARTED
            job.started = datetime.now()
        else:
            raise ValueError(f"Invalid value for status: {status}")

    def wait_for(self, *job_ids: uuid.UUID):
        """Wait for the jobs with the specified ids, also optionally sync their outputs with the pipeline value state."""

        self._wait_for(*job_ids)

        for job_id in job_ids:
            job = self._active_jobs[job_id]
            if job is None:
                raise Exception(f"Can't find job with id: {job_id}")
            if job.status == JobStatus.SUCCESS:
                job.job_log.percent_finished = 100

    @abc.abstractmethod
    def _wait_for(self, *job_ids: uuid.UUID):
        pass

    @abc.abstractmethod
    def queue_job(
        self,
        job_id: uuid.UUID,
        module: "KiaraModule",
        inputs: Mapping[str, uuid.UUID],
        outputs: ValueSetWritable,
        job_log: JobLog,
    ) -> str:
        pass
