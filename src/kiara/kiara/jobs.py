# -*- coding: utf-8 -*-
import structlog
import uuid
from typing import TYPE_CHECKING, Any, Mapping

from kiara.exceptions import JobConfigException
from kiara.models.module.jobs import JobConfig, ActiveJob, JobStatus
from kiara.models.module.manifest import Manifest
from kiara.models.values.value import ValueSet
from kiara.processing import ModuleProcessor
from kiara.processing.synchronous import SynchronousProcessor

if TYPE_CHECKING:
    from kiara import Kiara

logger = structlog.getLogger()


class JobsMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._processor: ModuleProcessor = SynchronousProcessor(kiara=self._kiara)

    def prepare_job_config(
        self, manifest: Manifest, inputs: Mapping[str, Any]
    ) -> JobConfig:

        module = self._kiara.create_module(manifest=manifest)
        job_config = JobConfig.create_from_module(data_registry=self._kiara.data_registry, module=module, inputs=inputs)

        return job_config

    def execute(self, manifest: Manifest, inputs: Mapping[str, Any], wait: bool=False) -> uuid.UUID:

        job_config = self.prepare_job_config(manifest=manifest, inputs=inputs)
        return self.execute_job(job_config, wait=wait)

    def execute_job(self, job_config: JobConfig, wait: bool=False) -> uuid.UUID:

        log = logger.bind(
            module_type=job_config.module_type,
            inputs={k: str(v) for k, v in job_config.inputs.items()},
            job_hash=job_config.model_data_hash,
        )

        stored_job = self._kiara.data_registry.find_matching_record(inputs_manifest=job_config)
        if stored_job is not None:
            log.debug("job.use.cached")
            raise NotImplementedError()
            return self._kiara.data_registry.load_values(values=stored_job.outputs)

        log.debug("job.execute", inputs=job_config.inputs)

        job_id = self._processor.process_job(job_config=job_config)

        if wait:
            self._processor.wait_for(job_id)

        return job_id

    def get_job_details(self, job_id: uuid.UUID) -> ActiveJob:
        return self._processor.get_job(job_id)

    def get_job_status(self, job_id: uuid.UUID) -> JobStatus:

        return self._processor.get_job_status(job_id=job_id)

    def retrieve_job(self, job_id: uuid.UUID, wait_for_finish: bool=False) -> ActiveJob:

        if wait_for_finish:
            self._processor.wait_for(job_id)

        job = self._processor.get_job(job_id=job_id)
        return job

    def retrieve_result(self, job_id: uuid.UUID) -> ValueSet:

        self._processor.wait_for(job_id)
        job = self._processor.get_job_record(job_id=job_id)

        results = self._kiara.data_registry.load_values(job.outputs)
        return results

    def execute_and_retrieve(self, manifest: Manifest, inputs: Mapping[str, Any], wait: bool=False) -> ValueSet:

        job_id = self.execute(manifest=manifest, inputs=inputs, wait=True)
        results = self.retrieve_result(job_id=job_id)
        return results

