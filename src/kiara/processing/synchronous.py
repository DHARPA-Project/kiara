# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid

from kiara.models.values.value import ValueMap, ValueMapWritable
from kiara.modules import KiaraModule
from kiara.processing import JobLog, JobStatus, ModuleProcessor, ProcessorConfig

try:
    pass
except Exception:
    pass


class SynchronousProcessorConfig(ProcessorConfig):

    pass


class SynchronousProcessor(ModuleProcessor):
    def _add_processing_task(
        self,
        job_id: uuid.UUID,
        module: "KiaraModule",
        inputs: ValueMap,
        outputs: ValueMapWritable,
        job_log: JobLog,
    ):

        self.job_status_updated(job_id=job_id, status=JobStatus.STARTED)
        try:
            module.process_step(inputs=inputs, outputs=outputs, job_log=job_log)
            # output_wrap._sync()
            self.job_status_updated(job_id=job_id, status=JobStatus.SUCCESS)
        except Exception as e:
            self.job_status_updated(job_id=job_id, status=e)

    def _wait_for(self, *job_ids: uuid.UUID):

        # jobs will always be finished, since we were waiting for them in the 'process' method
        return
