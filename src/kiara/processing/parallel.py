# -*- coding: utf-8 -*-
import typing
from concurrent.futures import Future, ThreadPoolExecutor
from pydantic import Field

from kiara.data import ValueSet
from kiara.module import KiaraModule, StepInputs, StepOutputs
from kiara.processing import JobLog, JobStatus, ModuleProcessor, ProcessorConfig

try:
    pass
except Exception:
    pass


class ThreadPoolProcessorConfig(ProcessorConfig):

    max_workers: typing.Optional[int] = Field(
        description="The max mount of workers for the thread pool.", default=None
    )


class ThreadPoolProcessor(ModuleProcessor):
    def __init__(self, max_workers: typing.Optional[int] = None, **kwargs):

        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: typing.Dict[str, Future] = {}

        super().__init__(**kwargs)

    def process(
        self,
        job_id: str,
        module: KiaraModule,
        inputs: ValueSet,
        outputs: ValueSet,
        job_log: JobLog,
    ):

        # TODO: ensure this is thread-safe
        future = self._executor.submit(
            self.wrap_process, job_id, module, inputs, outputs, job_log
        )
        self._futures[job_id] = future

    def wrap_process(
        self,
        job_id: str,
        module: KiaraModule,
        inputs: ValueSet,
        outputs: ValueSet,
        job_log: JobLog,
    ):

        wrapped_inputs = StepInputs(inputs=inputs)
        wrapped_outputs = StepOutputs(outputs=outputs)

        self.job_status_updated(job_id=job_id, status=JobStatus.STARTED)
        try:
            module.process_step(
                inputs=wrapped_inputs, outputs=wrapped_outputs, job_log=job_log
            )
            wrapped_outputs.sync()
            self.job_status_updated(job_id=job_id, status=JobStatus.SUCCESS)
        except Exception as e:
            self.job_status_updated(job_id=job_id, status=e)

    def wait_for(self, *job_ids: str) -> None:

        futures = []
        missing = []
        for job_id in job_ids:
            f = self._futures.get(job_id, None)
            if f is None:
                missing.append(job_id)
            else:
                futures.append(f)

        if missing:
            raise Exception(f"No job(s) running for id(s): {', '.join(missing)}")

        for f in futures:
            f.result()
