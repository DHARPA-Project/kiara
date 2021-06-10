# -*- coding: utf-8 -*-
import logging
import typing

from kiara import Pipeline, PipelineController
from kiara.data import Value
from kiara.events import PipelineInputEvent, PipelineOutputEvent, StepInputEvent
from kiara.processing import ModuleProcessor

log = logging.getLogger("kiara")


class ApiController(PipelineController):
    def __init__(
        self,
        pipeline: typing.Optional[Pipeline] = None,
        processor: typing.Optional[ModuleProcessor] = None,
    ):

        self._is_running: bool = False
        super().__init__(pipeline=pipeline, processor=processor)

    def process_pipeline(self):

        if self._is_running:
            log.debug("Pipeline running, doing nothing.")
            raise Exception("Pipeline already running.")

        self._is_running = True
        try:
            for stage in self.processing_stages:
                job_ids = []
                for step_id in stage:
                    if not self.can_be_processed(step_id):
                        if self.can_be_skipped(step_id):
                            continue
                        else:
                            raise Exception(
                                f"Required pipeline step '{step_id}' can't be processed, inputs not ready yet: {', '.join(self.invalid_inputs(step_id))}"
                            )
                    try:
                        if not self.get_step_outputs(step_id).items_are_valid():
                            print(f"PROCESSING: {step_id}")
                            print(f"OUTPUT: {self.get_step_outputs(step_id)}")
                            job_id = self.process_step(step_id)
                            job_ids.append(job_id)
                    except Exception as e:
                        # TODO: cancel running jobs?
                        log.error(
                            f"Processing of step '{step_id}' from pipeline '{self.pipeline.structure.pipeline_id}' failed: {e}"
                        )
                        return False
                self._processor.wait_for(*job_ids)
        finally:
            self._is_running = False

    def step_inputs_changed(self, event: StepInputEvent):

        for step_id in event.updated_step_inputs.keys():
            outputs = self.get_step_outputs(step_id)
            outputs.invalidate()

    def pipeline_inputs_changed(self, event: PipelineInputEvent):

        if self._is_running:
            raise NotImplementedError()

    def pipeline_outputs_changed(self, event: "PipelineOutputEvent"):

        if self.pipeline_is_finished():
            # TODO: check if something is running
            self._is_running = False

    def get_pipeline_input(self, step_id: str, field_name: str) -> Value:

        self.ensure_step(step_id)

        outputs = self.get_step_inputs(step_id)
        return outputs.get_value_obj(field_name=field_name)

    def get_pipeline_output(self, step_id: str, field_name: str) -> Value:

        self.ensure_step(step_id)

        outputs = self.get_step_outputs(step_id)
        return outputs.get_value_obj(field_name=field_name)

    def _process(self, step_id: str) -> typing.Union[None, bool, str]:
        """Process the specified step.

        Returns:
            'None', if no processing was needed, but the step output is valid, 'False' if it's not possible to process the step, or a string which represents the job id of the processing
        """

        if not self.can_be_processed(step_id):
            if self.can_be_skipped(step_id):
                return None
            else:
                raise Exception(
                    f"Required pipeline step '{step_id}' can't be processed, inputs not ready yet: {', '.join(self.invalid_inputs(step_id))}"
                )

        try:
            if not self.get_step_outputs(step_id).items_are_valid():
                print(f"PROCESSING: {step_id}")
                print(f"OUTPUT: {self.get_step_outputs(step_id)}")
                job_id = self.process_step(step_id)
                return job_id
            else:
                return None
        except Exception as e:
            # TODO: cancel running jobs?
            log.error(
                f"Processing of step '{step_id}' from pipeline '{self.pipeline.structure.pipeline_id}' failed: {e}"
            )

            return False

    def ensure_step(self, step_id: str) -> bool:
        """Ensure a step has valid outputs.

        Returns:
            'True' if the step now has valid outputs, 'False` if that's not possible because of invalid/missing inputs
        """

        if step_id not in self.pipeline.step_ids:
            raise Exception(f"No step with id: {step_id}")

        outputs = self.get_step_outputs(step_id=step_id)
        if outputs.items_are_valid():
            return True

        if self._is_running:
            log.debug("Pipeline running, doing nothing.")
            raise Exception("Pipeline already running.")

        self._is_running = True
        try:
            for stage in self.processing_stages:
                print(stage)
                job_ids: typing.List[str] = []
                finished = False
                if step_id in stage:
                    finished = True
                    job_id = self._process(step_id=step_id)
                    if job_id is None:
                        return True
                    if not job_id:
                        return False
                    job_ids.append(job_id)  # type: ignore
                else:
                    for s_id in stage:
                        job_id = self._process(step_id=s_id)
                        if job_id is None:
                            continue
                        elif job_id is False:
                            return False

                        job_ids.append(job_id)  # type: ignore

                self._processor.wait_for(*job_ids)
                if finished:
                    break

        finally:
            self._is_running = False

        return True
