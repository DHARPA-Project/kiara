# -*- coding: utf-8 -*-
from kiara.data import ValueSet
from kiara.module import KiaraModule, StepInputs, StepOutputs
from kiara.processing import ModuleProcessor


class SynchronousProcessor(ModuleProcessor):
    def process(
        self, job_id: str, module: KiaraModule, inputs: ValueSet, outputs: ValueSet
    ):

        input_wrap: StepInputs = StepInputs(inputs=inputs)
        output_wrap: StepOutputs = StepOutputs(outputs=outputs)

        self.job_status_updated(job_id=job_id, status=0)
        try:
            module.process_step(inputs=input_wrap, outputs=output_wrap)
            output_wrap._sync()
            self.job_status_updated(job_id=job_id, status=100)
        except Exception as e:
            self.job_status_updated(job_id=job_id, status=e)
