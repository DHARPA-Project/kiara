# -*- coding: utf-8 -*-
from typing import Union, Mapping, Any, Optional

import orjson

from kiara.kiara import DataRegistry
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.controller import SinglePipelineBatchController
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.models.values.value import ValueSet
from kiara.modules import KiaraModule, ValueSetSchema, KIARA_CONFIG
from kiara.processing import ModuleProcessor


class PipelineModule(KiaraModule):

    _config_cls = PipelineConfig
    _module_type_name = "pipeline"

    def __init__(
        self,
        module_config: Union[None, KIARA_CONFIG, Mapping[str, Any]] = None,
    ):
        self._module_processor: Optional[ModuleProcessor] = None
        super().__init__(module_config=module_config)

    def is_pipeline(cls) -> bool:
        return True

    def _set_module_processor(self, processor: ModuleProcessor):
        self._module_processor = processor

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

    def process(self, inputs: ValueSet, outputs: ValueSet):

        pipeline_structure: PipelineStructure = self.config.structure

        pipeline = Pipeline(structure=pipeline_structure, data_registry=outputs._data_registry)
        controller = SinglePipelineBatchController(pipeline=pipeline, processor=self._module_processor)

        pipeline.set_pipeline_inputs(inputs=inputs)
        controller.process_pipeline()

        outputs.set_values(**pipeline.get_current_pipeline_outputs())
