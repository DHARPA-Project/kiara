# -*- coding: utf-8 -*-
import orjson.orjson
import structlog
from typing import Optional

from kiara.models.events.pipeline import PipelineDetails, PipelineEvent
from kiara.models.module.pipeline.pipeline import Pipeline, PipelineListener
from kiara.utils import is_debug

logger = structlog.getLogger()


class PipelineController(PipelineListener):

    pass


class SinglePipelineController(PipelineController):
    def __init__(self, pipeline: Pipeline):

        self._pipeline: Pipeline = pipeline
        self._pipeline_details: Optional[PipelineDetails] = None

    @property
    def pipeline(self) -> Pipeline:
        return self._pipeline

    def current_pipeline_state(self) -> PipelineDetails:

        if self._pipeline_details is None:
            self._pipeline_details = self.pipeline.get_pipeline_details()
        return self._pipeline_details

    def can_be_processed(self, step_id: str) -> bool:
        """Check whether the step with the provided id is ready to be processed."""

        pipeline_state = self.current_pipeline_state()
        step_state = pipeline_state.step_states[step_id]

        return not step_state.invalid_details

    def can_be_skipped(self, step_id: str) -> bool:
        """Check whether the processing of a step can be skipped."""

        required = self.pipeline.structure.step_is_required(step_id=step_id)
        if required:
            required = self.can_be_processed(step_id)
        return required

    def _pipeline_event_occurred(self, event: PipelineEvent):

        self._pipeline_details = None

        dbg("============")
        dbg("EVENT")
        print(event.json(option=orjson.OPT_INDENT_2))

    def process_step(
        self, step_id: str, raise_exception: bool = False, wait: bool = False
    ) -> str:
        """Kick off processing for the step with the provided id.

        Arguments:
            step_id: the id of the step that should be started
        """

        step_inputs = self.get_step_inputs(step_id)

        # if the inputs are not valid, ignore this step
        if not step_inputs.items_are_valid():
            status = step_inputs.check_invalid()
            assert status is not None
            raise Exception(
                f"Can't execute step '{step_id}', invalid inputs: {', '.join(status.keys())}"
            )

        # get the output 'holder' objects, which we'll need to pass to the module
        step_outputs = self.get_step_outputs(step_id)
        # get the module object that holds the code that will do the processing
        step = self.get_step(step_id)

        job_id = self._processor.start(
            pipeline_id=self.pipeline.id,
            pipeline_name=self.pipeline.title,
            step_id=step_id,
            module=step.module,
            inputs=step_inputs,
            outputs=step_outputs,
        )
        self._job_ids[step_id] = job_id

        if wait:
            self.wait_for_jobs(job_id, sync_outputs=True)

        return job_id


class SinglePipelineBatchController(SinglePipelineController):
    """A [PipelineController][kiara.models.modules.pipeline.controller.PipelineController] that executes all pipeline steps non-interactively.

    This is the default implementation of a ``PipelineController``, and probably the most simple implementation of one.
    It waits until all inputs are set, after which it executes all pipeline steps in the required order.

    Arguments:
        pipeline: the pipeline to control
        auto_process: whether to automatically start processing the pipeline as soon as the input set is valid
    """

    def __init__(
        self,
        pipeline: Pipeline,
        auto_process: bool = True,
    ):

        self._auto_process: bool = auto_process
        self._is_running: bool = False
        super().__init__(pipeline=pipeline)

    @property
    def auto_process(self) -> bool:
        return self._auto_process

    @auto_process.setter
    def auto_process(self, auto_process: bool):
        self._auto_process = auto_process

    def process_pipeline(self):

        if self._is_running:
            logger.debug(
                "ignore.pipeline_process",
                reason="Pipeline already running.",
                pipeline_id=self.pipeline.pipeline_id,
            )
            raise Exception("Pipeline already running.")

        self._is_running = True
        try:
            for stage in self.pipeline.structure.processing_stages:
                job_ids = []
                for step_id in stage:

                    if not self.can_be_processed(step_id):
                        if self.can_be_skipped(step_id):
                            continue
                        else:
                            invalid_inputs = (
                                self.pipeline.get_pipeline_details().invalid_details.keys()
                            )
                            assert invalid_inputs
                            raise Exception(
                                f"Required pipeline step '{step_id}' can't be processed, inputs not ready yet: {', '.join(invalid_inputs)}"
                            )
                    try:
                        job_id = self.process_step(step_id)
                        job_ids.append(job_id)
                    except Exception as e:
                        # TODO: cancel running jobs?
                        if is_debug():
                            import traceback

                            traceback.print_exc()
                        logger.error(
                            "error.processing.pipeline",
                            step_id=step_id,
                            pipeline_id=self.pipeline.pipeline_id,
                            error=e,
                        )
                        return False

                self._processor.wait_for(*job_ids)
                # for j_id in job_ids:
                #     job = self._processor.get_job_details(j_id)
                #     assert job is not None
                #     if job.error:
                #         print(job.error)
        finally:
            self._is_running = False

    def step_inputs_changed(self, event: "StepInputEvent"):

        if self._is_running:
            log.debug("Pipeline running, doing nothing.")
            return

        if not self.pipeline_is_ready():
            log.debug(f"Pipeline not ready after input event: {event}")
            return

        if self._auto_process:
            self.process_pipeline()

    def pipeline_outputs_changed(self, event: "PipelineOutputEvent"):

        if self.pipeline_is_finished():
            # TODO: check if something is running
            self._is_running = False
