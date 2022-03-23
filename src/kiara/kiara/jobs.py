# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Dict, Any, Mapping

import orjson

from kiara.models.module.jobs import JobConfig, JobLog
from kiara.models.module.manifest import Manifest, LoadConfig
from kiara.models.values.value import ValuePedigree, ValueSet, ValueSetWritable
import structlog
from kiara.exceptions import JobConfigException

if TYPE_CHECKING:
    from kiara import Kiara

logger = structlog.getLogger()

class JobsMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

    def prepare_job_config(self, manifest: Manifest, inputs: Mapping[str, Any]) -> JobConfig:

        module = self._kiara.create_module(manifest=manifest)
        augmented_inputs = module.augment_inputs(inputs=inputs)

        try:
            job_inputs: ValueSet = self._kiara.data_registry.create_valueset(
                data=augmented_inputs, schema=module.inputs_schema
            )

        except Exception as e:
            raise JobConfigException(e, manifest=manifest, inputs=inputs)

        if not job_inputs.all_items_valid:
            invalid_details = job_inputs.check_invalid()
            raise JobConfigException(
                msg=f"Can't process module '{manifest.module_type}', input field(s) not valid: {', '.join(invalid_details.keys())}", manifest=manifest, inputs=inputs
                # type: ignore
            )

        job = JobConfig(module_type=manifest.module_type, module_config=manifest.module_config,
                        inputs=job_inputs)  # type: ignore
        return job

    def execute(self, manifest: Manifest, inputs: Mapping[str, Any]):

        job = self.prepare_job_config(manifest=manifest, inputs=inputs)
        return self.execute_job(job)

    def execute_job(self, job_config: JobConfig):

        log = logger.bind(module_type=job_config.module_type, inputs={k: str(v.value_id) for k, v in job_config.inputs.items()}, job_hash=job_config.model_data_hash)

        stored_job = self._kiara.data_registry.find_job_record(job_config)
        if stored_job is not None:
            log.debug("job.use.cached")
            return self._kiara.data_registry.load_valueset(values=stored_job.outputs)

        log.debug("job.execute", inputs=job_config.inputs)
        environments = {env_name: env.model_data_hash for env_name, env in self._kiara.environments.items()}

        result_pedigree = ValuePedigree(
            kiara_id=self._kiara.id, module_type=job_config.module_type, module_config=job_config.module_config, inputs={field: job_config.inputs.get_value_obj(field).value_id for field in job_config.inputs.field_names}, environments=environments
        )

        module = self._kiara.create_module(manifest=job_config)

        outputs = ValueSetWritable.create_from_schema(
            kiara=self._kiara, schema=module.outputs_schema, pedigree=result_pedigree
        )
        job_log = JobLog()

        module.process_step(inputs=job_config.inputs, outputs=outputs, job_log=job_log)
        return outputs



