# -*- coding: utf-8 -*-
import uuid
from typing import TYPE_CHECKING, Any, Dict, Mapping, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.module.jobs import JobLog
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.controller import SinglePipelineBatchController
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.values.value import ValueMap, ValueMapWritable
from kiara.modules import KIARA_CONFIG, KiaraModule, ValueMapSchema

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


if TYPE_CHECKING:
    from kiara.models.module.operation import Operation
    from kiara.models.module.pipeline.structure import PipelineStructure
    from kiara.registries.jobs import JobRegistry


class PipelineModule(KiaraModule):
    """A utility module to run multiple connected inner-modules and present it as its own entity."""

    _config_cls = PipelineConfig
    _module_type_name = "pipeline"

    def __init__(
        self,
        module_config: Union[None, KIARA_CONFIG, Mapping[str, Any]] = None,
    ):
        self._job_registry: Union[JobRegistry, None] = None
        super().__init__(module_config=module_config)

    @classmethod
    def is_pipeline(cls) -> bool:
        return True

    def _set_job_registry(self, job_registry: "JobRegistry"):
        self._job_registry = job_registry

    @property
    def operation(self) -> "Operation":

        if self._operation is not None:
            return self._operation

        from kiara.models.module.operation import Operation

        self._operation = Operation.create_from_module(self, doc=self.config.doc)
        return self._operation

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        pipeline_structure: PipelineStructure = self.config.structure
        inputs_schema = pipeline_structure.pipeline_inputs_schema
        return inputs_schema

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:
        pipeline_structure: PipelineStructure = self.config.structure
        return pipeline_structure.pipeline_outputs_schema

    def process(self, inputs: ValueMap, outputs: ValueMapWritable, job_log: JobLog):

        pipeline_structure: PipelineStructure = self.config.structure

        pipeline = Pipeline(structure=pipeline_structure, kiara=outputs._kiara)

        assert self._job_registry is not None
        controller = SinglePipelineBatchController(
            pipeline=pipeline, job_registry=self._job_registry
        )

        pipeline.set_pipeline_inputs(inputs=inputs)
        step_details = controller.process_pipeline()

        errors: Dict[str, Union[Exception, uuid.UUID]] = {}
        for step_id, details in step_details.items():
            if isinstance(details, Exception):
                errors[step_id] = details
            else:
                job = self._job_registry.get_job(details)
                if job.error:
                    if job._exception:
                        errors[step_id] = job._exception
                    else:
                        errors[step_id] = Exception(job.error)

        if errors:
            msg = "Error processing pipeline:"
            for f, e in errors.items():
                msg = f"{msg}\n  - {f}: {e}"

            raise KiaraProcessingException(f"Errors while processing pipeline: {msg}")

        # TODO: resolve values first?
        outputs.set_values(**pipeline.get_current_pipeline_outputs())
