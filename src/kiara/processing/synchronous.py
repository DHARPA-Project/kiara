# -*- coding: utf-8 -*-


from kiara.data import ValueSet
from kiara.module import KiaraModule
from kiara.processing import ModuleProcessor, ProcessorConfig

try:
    pass
except Exception:
    pass


class SynchronousProcessorConfig(ProcessorConfig):

    pass


class SynchronousProcessor(ModuleProcessor):
    def process(
        self, job_id: str, module: KiaraModule, inputs: ValueSet, outputs: ValueSet
    ):

        self.job_status_updated(job_id=job_id, status=0)
        try:
            module.process_step(inputs=inputs, outputs=outputs)
            # output_wrap._sync()
            self.job_status_updated(job_id=job_id, status=100)
        except Exception as e:
            self.job_status_updated(job_id=job_id, status=e)

    def wait_for(self, *job_ids: str):

        # jobs will always be finished, since we were waiting for them in the 'process' method
        return
