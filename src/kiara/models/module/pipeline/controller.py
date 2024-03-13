# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from typing import Callable, Dict, Mapping, Union

import structlog

from kiara.models.events.pipeline import PipelineEvent, PipelineState
from kiara.models.module.pipeline.pipeline import Pipeline, PipelineListener
from kiara.models.module.pipeline.stages import PipelineStage
from kiara.registries.jobs import JobRegistry
from kiara.utils import log_exception

logger = structlog.getLogger()


class PipelineController(PipelineListener):

    pass


class SinglePipelineController(PipelineController):
    def __init__(
        self, job_registry: JobRegistry, pipeline: Union[Pipeline, None] = None
    ):

        self._pipeline: Union[Pipeline, None] = None
        self._job_registry: JobRegistry = job_registry
        self._pipeline_details: Union[PipelineState, None] = None

        if pipeline is not None:
            self.pipeline = pipeline

    @property
    def pipeline(self) -> Pipeline:

        if self._pipeline is None:
            raise Exception("Pipeline not set (yet).")
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline: Pipeline):

        if self._pipeline is not None:
            # TODO: destroy object?
            self._pipeline._listeners.clear()

        self._pipeline = pipeline
        if self._pipeline is not None:
            self._pipeline.add_listener(self)

    def current_pipeline_state(self) -> PipelineState:

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

        if event.pipeline_id != self.pipeline.pipeline_id:
            return

        self._pipeline_details = None

    def set_processing_results(
        self, job_ids: Mapping[str, uuid.UUID]
    ) -> Mapping[uuid.UUID, uuid.UUID]:
        """
        Set the processing results as values of the approrpiate step outputs.

        Returns:
        -------
            a dict with the result value id as key, and the id of the job that produced it as value
        """
        self._job_registry.wait_for(*job_ids.values())

        result: Dict[uuid.UUID, uuid.UUID] = {}
        combined_outputs = {}
        for step_id, job_id in job_ids.items():
            record = self._job_registry.get_job_record(job_id=job_id)
            if record is None:
                continue
            combined_outputs[step_id] = record.outputs
            for output_id in record.outputs.values():
                result[output_id] = job_id

        self.pipeline.set_multiple_step_outputs(
            changed_outputs=combined_outputs, notify_listeners=True
        )

        return result

    def pipeline_is_ready(self) -> bool:
        """
        Return whether the pipeline is ready to be processed.

        A ``True`` result means that all pipeline inputs are set with valid values, and therefore every step within the
        pipeline can be processed.

        Returns:
        -------
            whether the pipeline can be processed as a whole (``True``) or not (``False``)
        """
        pipeline_inputs = self.pipeline._all_values.get_alias("pipeline.inputs")
        assert pipeline_inputs is not None
        return pipeline_inputs.all_items_valid

    def process_step(self, step_id: str, wait: bool = False) -> uuid.UUID:
        """
        Kick off processing for the step with the provided id.

        Arguments:
        ---------
            step_id: the id of the step that should be started
        """

        from kiara.models.module.jobs import PipelineMetadata

        job_config = self.pipeline.create_job_config_for_step(step_id)

        # pipeline_metadata = {
        #     "is_pipeline_step": True,
        #     "step_id": step_id,
        #     "pipeline_id": self.pipeline.pipeline_id,
        # }

        pipeline_metadata = PipelineMetadata(
            pipeline_id=self.pipeline.pipeline_id, step_id=step_id
        )
        job_config.pipeline_metadata = pipeline_metadata

        job_id = self._job_registry.execute_job(job_config=job_config)
        # job_id = self._processor.create_job(job_config=job_config)
        # self._processor.queue_job(job_id=job_id)

        if wait:
            self._job_registry.wait_for(job_id)

        return job_id


class SinglePipelineBatchController(SinglePipelineController):
    """
    A [PipelineController][kiara.models.modules.pipeline.controller.PipelineController] that executes all pipeline steps non-interactively.

    This is the default implementation of a ``PipelineController``, and probably the most simple implementation of one.
    It waits until all inputs are set, after which it executes all pipeline steps in the required order.

    Arguments:
    ---------
        pipeline: the pipeline to control
        auto_process: whether to automatically start processing the pipeline as soon as the input set is valid
    """

    def __init__(
        self,
        pipeline: Pipeline,
        job_registry: JobRegistry,
        auto_process: bool = True,
    ):

        self._auto_process: bool = auto_process
        self._is_running: bool = False
        super().__init__(pipeline=pipeline, job_registry=job_registry)

    @property
    def auto_process(self) -> bool:
        return self._auto_process

    @auto_process.setter
    def auto_process(self, auto_process: bool):
        self._auto_process = auto_process

    def process_pipeline(
        self, event_callback: Union[Callable, None] = None
    ) -> Mapping[str, Union[uuid.UUID, Exception]]:

        log = logger.bind(pipeline_id=self.pipeline.pipeline_id)
        if self._is_running:
            log.debug(
                "ignore.pipeline_process",
                reason="Pipeline already running.",
            )
            raise Exception("Pipeline already running.")

        log.debug("execute.pipeline")
        self._is_running = True
        all_job_ids: Dict[str, Union[Exception, uuid.UUID]] = {}
        try:
            stages = PipelineStage.extract_stages(
                self.pipeline.structure, stages_extraction_type="early"
            )
            for idx, stage in enumerate(stages, start=1):

                if event_callback:
                    event_callback(f"start processing pipeline stage: {idx}")

                log.debug(
                    "execute.pipeline.stage",
                    stage=idx,
                )

                job_ids = {}
                for step_id in stage:
                    if event_callback:
                        event_callback(f"start processing pipeline step: {step_id}")

                    log.debug(
                        "execute.pipeline.step",
                        step_id=step_id,
                    )

                    try:
                        job_id = self.process_step(step_id)
                        job_ids[step_id] = job_id
                        if event_callback:
                            event_callback(f"finished processing step '{step_id}'")
                    except Exception as e:
                        all_job_ids[step_id] = e
                        # TODO: cancel running jobs?
                        log_exception(e)
                        log.error(
                            "error.processing.pipeline",
                            step_id=step_id,
                            error=e,
                        )
                        if event_callback:
                            event_callback(f"Error processing step '{step_id}': {e}")

                self.set_processing_results(job_ids=job_ids)
                log.debug(
                    "execute_finished.pipeline.stage",
                    stage=idx,
                )
                if event_callback:
                    event_callback(f"finished processing pipeline stage: {idx}")
                all_job_ids.update(job_ids)

        finally:
            self._is_running = False

        log.debug("execute_finished.pipeline")
        if event_callback:
            event_callback("finished processing pipeline")
        return all_job_ids
