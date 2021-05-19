# -*- coding: utf-8 -*-
import typing
from concurrent.futures import Future, ThreadPoolExecutor

from kiara.data import ValueSet
from kiara.module import KiaraModule, StepInputs, StepOutputs
from kiara.processing import ModuleProcessor


class ThreadPoolProcessor(ModuleProcessor):
    def __init__(self, max_workers: typing.Optional[int] = None, **kwargs):

        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: typing.Dict[str, Future] = {}

        super().__init__(**kwargs)

    def process(
        self, job_id: str, module: KiaraModule, inputs: ValueSet, outputs: ValueSet
    ):

        # TODO: ensure this is thread-safe
        future = self._executor.submit(
            self.wrap_process, job_id, module, inputs, outputs
        )
        self._futures[job_id] = future

    def wrap_process(
        self, job_id: str, module: KiaraModule, inputs: ValueSet, outputs: ValueSet
    ):

        wrapped_inputs = StepInputs(inputs=inputs)
        wrapped_outputs = StepOutputs(outputs=outputs)

        self.job_status_updated(job_id=job_id, status=0)
        try:
            module.process_step(inputs=wrapped_inputs, outputs=wrapped_outputs)
            wrapped_outputs.sync()
            self.job_status_updated(job_id=job_id, status=100)
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
