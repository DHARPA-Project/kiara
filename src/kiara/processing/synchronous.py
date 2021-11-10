# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from kiara.data.values.value_set import ValueSet
from kiara.module import KiaraModule
from kiara.processing import JobLog, JobStatus
from kiara.processing.processor import ModuleProcessor, ProcessorConfig

try:
    pass
except Exception:
    pass


class SynchronousProcessorConfig(ProcessorConfig):

    pass


class SynchronousProcessor(ModuleProcessor):
    def process(
        self,
        job_id: str,
        module: KiaraModule,
        inputs: ValueSet,
        outputs: ValueSet,
        job_log: JobLog,
    ):

        self.job_status_updated(job_id=job_id, status=JobStatus.STARTED)
        try:
            module.process_step(inputs=inputs, outputs=outputs, job_log=job_log)
            # output_wrap._sync()
            self.job_status_updated(job_id=job_id, status=JobStatus.SUCCESS)
        except Exception as e:
            self.job_status_updated(job_id=job_id, status=e)

    def _wait_for(self, *job_ids: str):

        # jobs will always be finished, since we were waiting for them in the 'process' method
        return
