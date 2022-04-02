# -*- coding: utf-8 -*-
import structlog
import uuid
from typing import Mapping, Optional

from kiara.models.events.pipeline import PipelineDetails, PipelineEvent
from kiara.models.module.pipeline.pipeline import Pipeline, PipelineListener
from kiara.processing import ModuleProcessor
from kiara.utils import is_debug

logger = structlog.getLogger()


class PipelineController(PipelineListener):

    pass


class SinglePipelineController(PipelineController):
    def __init__(self, pipeline: Pipeline, processor: ModuleProcessor):

        self._pipeline: Pipeline = pipeline
        self._processor: ModuleProcessor = processor
        self._pipeline.add_listener(self)
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

    def set_processing_results(self, job_ids: Mapping[str, uuid.UUID]):

        self._processor.wait_for(*job_ids.values())

        combined_outputs = {}
        for step_id, job_id in job_ids.items():
            record = self._processor.get_job_record(job_id=job_id)
            combined_outputs[step_id] = record.outputs

        self.pipeline.set_multiple_step_outputs(
            changed_outputs=combined_outputs, notify_listeners=True
        )

    def pipeline_is_ready(self) -> bool:
        """Return whether the pipeline is ready to be processed.

        A ``True`` result means that all pipeline inputs are set with valid values, and therefore every step within the
        pipeline can be processed.

        Returns:
            whether the pipeline can be processed as a whole (``True``) or not (``False``)
        """

        pipeline_inputs = self.pipeline._all_values.get_alias("pipeline.inputs")
        assert pipeline_inputs is not None
        return pipeline_inputs.all_items_valid

    def process_step(self, step_id: str, wait: bool = False) -> uuid.UUID:
        """Kick off processing for the step with the provided id.

        Arguments:
            step_id: the id of the step that should be started
        """

        job_config = self.pipeline.create_job_config_for_step(step_id)

        job_id = self._processor.create_job(job_config=job_config)
        self._processor.queue_job(job_id=job_id)

        if wait:
            self._processor.wait_for(job_id)

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
        processor: ModuleProcessor,
        auto_process: bool = True,
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
            logger.debug(
                "ignore.pipeline_process",
                reason="Pipeline already running.",
                pipeline_id=self.pipeline.pipeline_id,
            )
            raise Exception("Pipeline already running.")

        logger.debug("execute.pipeline", pipeline_id=self.pipeline.pipeline_id)
        self._is_running = True
        try:
            for idx, stage in enumerate(
                self.pipeline.structure.processing_stages, start=1
            ):

                logger.debug(
                    "execute.pipeline.stage",
                    pipeline_id=self.pipeline.pipeline_id,
                    stage=idx,
                )

                job_ids = {}
                for step_id in stage:

                    logger.debug(
                        "execute.pipeline.step",
                        pipeline_id=self.pipeline.pipeline_id,
                        step_id=step_id,
                    )

                    try:
                        job_id = self.process_step(step_id)
                        job_ids[step_id] = job_id
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

                self.set_processing_results(job_ids=job_ids)
                logger.debug(
                    "execute_finished.pipeline.stage",
                    pipeline_id=self.pipeline.pipeline_id,
                    stage=idx,
                )

        finally:
            self._is_running = False

        logger.debug("execute_finished.pipeline", pipeline_id=self.pipeline.pipeline_id)