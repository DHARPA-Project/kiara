# -*- coding: utf-8 -*-

from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.models.values.value import ValueSet
from kiara.modules import KiaraModule, ValueSetSchema


class PipelineModule(KiaraModule):

    _config_cls = PipelineConfig

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
        print(pipeline_structure)
        raise NotImplementedError()
