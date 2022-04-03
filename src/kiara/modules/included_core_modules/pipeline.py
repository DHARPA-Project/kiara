# -*- coding: utf-8 -*-
from typing import Any, Mapping, Optional, Union, TYPE_CHECKING

from kiara.models.module.jobs import JobLog
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.controller import SinglePipelineBatchController
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.models.values.value import ValueSet
from kiara.modules import KIARA_CONFIG, KiaraModule, ValueSetSchema

if TYPE_CHECKING:
    from kiara.kiara.job_registry import JobRegistry


class PipelineModule(KiaraModule):

    _config_cls = PipelineConfig
    _module_type_name = "pipeline"

    def __init__(
        self,
        module_config: Union[None, KIARA_CONFIG, Mapping[str, Any]] = None,
    ):
        self._job_registry: Optional[JobRegistry] = None
        super().__init__(module_config=module_config)

    def is_pipeline(cls) -> bool:
        return True

    def _set_job_registry(self, job_registry: "JobRegistry"):
        self._job_registry = job_registry

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        pipeline_structure: PipelineStructure = self.config.structure
        return pipeline_structure.pipeline_inputs_schema

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:
        pipeline_structure: PipelineStructure = self.config.structure
        return pipeline_structure.pipeline_outputs_schema

    def process(self, inputs: ValueSet, outputs: ValueSet, job_log: JobLog):

        pipeline_structure: PipelineStructure = self.config.structure

        pipeline = Pipeline(
            structure=pipeline_structure, data_registry=outputs._data_registry
        )
        controller = SinglePipelineBatchController(
            pipeline=pipeline, job_registry=self._job_registry
        )

        pipeline.set_pipeline_inputs(inputs=inputs)
        controller.process_pipeline()

        # TODO: resolve values first?
        outputs.set_values(**pipeline.get_current_pipeline_outputs())
