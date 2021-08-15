# -*- coding: utf-8 -*-
import logging
import typing

from kiara.pipeline.controller import PipelineController
from kiara.utils import is_debug

if typing.TYPE_CHECKING:
    from kiara.events import PipelineInputEvent, PipelineOutputEvent, StepInputEvent
    from kiara.pipeline.pipeline import Pipeline
    from kiara.processing.processor import ModuleProcessor

log = logging.getLogger("kiara")


class BatchController(PipelineController):
    """A [PipelineController][kiara.pipeline.controller.PipelineController] that executes all pipeline steps non-interactively.

    This is the default implementation of a ``PipelineController``, and probably the most simple implementation of one.
    It waits until all inputs are set, after which it executes all pipeline steps in the required order.

    Arguments:
        pipeline: the pipeline to control
        auto_process: whether to automatically start processing the pipeline as soon as the input set is valid
    """

    def __init__(
        self,
        pipeline: typing.Optional["Pipeline"] = None,
        auto_process: bool = True,
        processor: typing.Optional["ModuleProcessor"] = None,
    ):

        self._auto_process: bool = auto_process
        self._is_running: bool = False
        super().__init__(pipeline=pipeline, processor=processor)

    @property
    def auto_process(self) -> bool:
        return self._auto_process

    @auto_process.setter
    def auto_process(self, auto_process: bool):
        self._auto_process = auto_process

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
                        job_id = self.process_step(step_id)
                        job_ids.append(job_id)
                    except Exception as e:
                        # TODO: cancel running jobs?
                        if is_debug():
                            import traceback

                            traceback.print_stack()
                        log.error(
                            f"Processing of step '{step_id}' from pipeline '{self.pipeline.structure.pipeline_id}' failed: {e}"
                        )
                        return False
                self._processor.wait_for(*job_ids)
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


class BatchControllerManual(PipelineController):
    """A [PipelineController][kiara.pipeline.controller.PipelineController] that executes all pipeline steps non-interactively.

    This is the default implementation of a ``PipelineController``, and probably the most simple implementation of one.
    It waits until all inputs are set, after which it executes all pipeline steps in the required order.

    Arguments:
        pipeline: the pipeline to control
        auto_process: whether to automatically start processing the pipeline as soon as the input set is valid
    """

    def __init__(
        self,
        pipeline: typing.Optional["Pipeline"] = None,
        processor: typing.Optional["ModuleProcessor"] = None,
    ):

        self._finished_until: typing.Optional[int] = None
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
                        job_id = self.process_step(step_id)
                        job_ids.append(job_id)
                    except Exception as e:
                        # TODO: cancel running jobs?
                        if is_debug():
                            import traceback

                            traceback.print_stack()
                        log.error(
                            f"Processing of step '{step_id}' from pipeline '{self.pipeline.title}' failed: {e}"
                        )
                        return False
                self._processor.wait_for(*job_ids)
        finally:
            self._is_running = False

    def step_inputs_changed(self, event: "StepInputEvent"):

        if self._is_running:
            log.debug("Pipeline running, doing nothing.")
            return

        if not self.pipeline_is_ready():
            log.debug(f"Pipeline not ready after input event: {event}")
            return

    def pipeline_inputs_changed(self, event: "PipelineInputEvent"):

        self._finished_until = None

    def pipeline_outputs_changed(self, event: "PipelineOutputEvent"):

        if self.pipeline_is_finished():
            # TODO: check if something is running
            self._is_running = False

    def process_stage(self, stage_nr: int):

        if self._is_running:
            log.debug("Pipeline running, doing nothing.")
            raise Exception("Pipeline already running.")

        self._is_running = True
        try:
            for idx, stage in enumerate(self.processing_stages):

                if idx + 1 > stage_nr:
                    break

                if self._finished_until is not None and idx <= self._finished_until:
                    continue

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
                        job_id = self.process_step(step_id)
                        job_ids.append(job_id)
                    except Exception as e:
                        # TODO: cancel running jobs?
                        if is_debug():
                            import traceback

                            traceback.print_stack()
                        log.error(
                            f"Processing of step '{step_id}' from pipeline '{self.pipeline.title}' failed: {e}"
                        )
                        return False
                self._processor.wait_for(*job_ids)
                self._finished_until = idx
        finally:
            self._is_running = False
