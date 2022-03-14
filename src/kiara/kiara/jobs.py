# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Dict, Any

import orjson

from kiara.models.module.jobs import JobConfig, JobLog
from kiara.models.module.manifest import Manifest
from kiara.models.values.value import ValuePedigree, ValueSet, ValueSetWritable
import structlog

from kiara.value_types.included_core_types.persistence import LoadConfig

if TYPE_CHECKING:
    from kiara import Kiara

logger = structlog.getLogger()

class JobsMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

    def execute(self, manifest: Manifest, inputs: Dict[str, Any]):

        module = self._kiara.create_module(manifest=manifest)
        augmented_inputs = module.augment_inputs(inputs=inputs)

        job_inputs: ValueSet = self._kiara.data_registry.create_valueset(
            data=augmented_inputs, schema=module.inputs_schema
        )

        if not job_inputs.all_items_valid:
            invalid_details = job_inputs.check_invalid()
            raise Exception(
                f"Can't process module '{manifest.module_type}', input field(s) not valid: {', '.join(invalid_details.keys())}"  # type: ignore
            )

        job = JobConfig(module_type=manifest.module_type, module_config=manifest.module_config, inputs=job_inputs)

        log = logger.bind(module_type=manifest.module_type, inputs={k: str(v.value_id) for k, v in job.inputs.items()}, job_hash=job.model_data_hash)


        stored_job = self._kiara.data_store.retrieve_job(job)
        if stored_job is not None:
            log.debug("job.use.cached")
            return self._kiara.data_registry.load_valueset(values=stored_job.outputs)

        log.debug("job.execute")
        environments = {env_name: env.model_data_hash for env_name, env in self._kiara.environments.items()}

        result_pedigree = ValuePedigree(
            kiara_id=self._kiara.id, module_type=job.module_type, module_config=job.module_config, inputs={field: job_inputs.get_value_obj(field).value_id for field in job_inputs.field_names}, environments=environments
        )
        outputs = ValueSetWritable.create_from_schema(
            kiara=self._kiara, schema=module.outputs_schema, pedigree=result_pedigree
        )
        job_log = JobLog()

        module.process_step(inputs=job_inputs, outputs=outputs, job_log=job_log)
        return outputs


    def load_data_from_config(self, load_config: LoadConfig) -> Any:

        logger.debug("value.load", module=load_config.module_type)

        result = self.execute(manifest=load_config, inputs=load_config.inputs)

        # data = result.get_value_data(load_config.output_name)
        result_value = result.get_value_obj(field_name=load_config.output_name)
        return result_value.data
