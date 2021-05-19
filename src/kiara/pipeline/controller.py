# -*- coding: utf-8 -*-
import logging
import typing

from kiara.data import Value, ValueSet
from kiara.pipeline.listeners import PipelineListener
from kiara.pipeline.structure import PipelineStep
from kiara.processing import ModuleProcessor
from kiara.processing.synchronous import SynchronousProcessor

if typing.TYPE_CHECKING:
    from kiara.events import PipelineOutputEvent, StepInputEvent
    from kiara.pipeline.pipeline import Pipeline, PipelineState, StepStatus

log = logging.getLogger("kiara")


class PipelineController(PipelineListener):
    """An object that controls how a [Pipeline][kiara.pipeline.pipeline.Pipeline] should react to events related to it's inputs/outputs.

    This is the base for the central controller class that needs to be implemented by a *Kiara* frontend. The default implementation
    that is used if no ``PipelineController`` is provided in a [Pipeline][kiara.pipeline.pipeline.Pipeline] constructor
    is the [BatchController][kiara.pipeline.controller.BatchController], which basically waits until all required inputs are
    set, and then processes all pipeline steps in one go (in the right order).

    The pipeline object to control can be set either in the constructor, or via the ``set_pipeline`` method. But only once,
    every subsequent attempt to set a pipeline will raise an Exception.

    If you want to implement your own controller, you have to override at least one of the (empty) event hook methods:

      - [``pipeline_inputs_changed``][kiara.pipeline.controller.PipelineController.pipeline_inputs_changed]
      - [``pipeline_outputs_changed``][kiara.pipeline.controller.PipelineController.pipeline_outputs_changed]
      - [``step_inputs_changed``][kiara.pipeline.controller.PipelineController.step_inputs_changed]
      - [``step_outputs_changed``][kiara.pipeline.controller.PipelineController.step_outputs_changed]

    Arguments:
        pipeline (Pipeline): the pipeline object to control

    """

    def __init__(
        self,
        pipeline: typing.Optional["Pipeline"] = None,
        processor: typing.Optional[ModuleProcessor] = None,
    ):
        self._pipeline: typing.Optional[Pipeline] = None
        if processor is None:
            processor = SynchronousProcessor()
        self._processor: ModuleProcessor = processor
        self._running_steps: typing.Mapping[str, str] = {}
        """A map of all currently running steps, and their job id."""

        if pipeline is not None:
            self.set_pipeline(pipeline)

    @property
    def pipeline(self) -> "Pipeline":
        """Return the pipeline this controller, well, ...controls..."""

        if self._pipeline is None:
            raise Exception("Pipeline not set yet.")
        return self._pipeline

    @property
    def pipeline_status(self) -> "StepStatus":
        return self.pipeline.status

    def set_pipeline(self, pipeline: "Pipeline"):
        """Set the pipeline object for this controller.

        Only one pipeline can be set, once.

        Arguments:
            pipeline: the pipeline object
        """
        if self._pipeline is not None:
            raise Exception("Pipeline already set.")
        self._pipeline = pipeline

    @property
    def processing_stages(self) -> typing.List[typing.List[str]]:
        """Return the processing stage order of the pipeline.

        Returns:
            a list of lists of step ids
        """

        return self.pipeline.structure.processing_stages

    def get_step(self, step_id: str) -> PipelineStep:
        """Return the step object for the provided id.

        Arguments:
            step_id: the step id
        Returns:
            the step object
        """

        return self.pipeline.get_step(step_id)

    def get_step_inputs(self, step_id: str) -> ValueSet:
        """Return the inputs object for the pipeline."""

        return self.pipeline.get_step_inputs(step_id)

    def get_step_outputs(self, step_id: str) -> ValueSet:
        """Return the outputs object for the pipeline."""

        return self.pipeline.get_step_outputs(step_id)

    def get_step_input(self, step_id: str, input_name: str) -> Value:
        """Get the (current) input value for a specified step and input field name."""

        item = self.get_step_inputs(step_id).get_value_obj(input_name)
        assert item is not None
        return item

    def get_step_output(self, step_id: str, output_name: str) -> Value:
        """Get the (current) output value for a specified step and output field name."""

        item = self.get_step_outputs(step_id).get_value_obj(output_name)
        assert item is not None
        return item

    def get_current_pipeline_state(self) -> "PipelineState":
        """Return a description of the current pipeline state.

        This methods creates a new [PipelineState][kiara.pipeline.pipeline.PipelineState] object when called, containing
        the pipeline structure as well as metadata about pipeline as well as step inputs and outputs.

        Returns:
            an object outlining the current pipeline state
        """

        return self.pipeline.get_current_state()

    @property
    def pipeline_inputs(self) -> ValueSet:
        """Return the inputs object for this pipeline."""

        return self.pipeline._pipeline_inputs

    @pipeline_inputs.setter
    def pipeline_inputs(self, inputs: typing.Mapping[str, typing.Any]) -> None:
        """Set one, several or all inputs for this pipeline."""

        self.set_pipeline_inputs(**inputs)

    @property
    def pipeline_outputs(self) -> ValueSet:
        """Return the (current) pipeline outputs object for this pipeline."""

        return self.pipeline._pipeline_outputs

    def can_be_processed(self, step_id: str) -> bool:
        """Check whether the step with the provided id is ready to be processed."""

        result = True
        step_inputs = self.get_step_inputs(step_id=step_id)
        for input_name in step_inputs.get_all_field_names():

            value = step_inputs.get_value_obj(input_name)

            if not value.item_is_valid():
                result = False
                break

        return result

    def can_be_skipped(self, step_id: str) -> bool:
        """Check whether the processing of a step can be skipped."""

        result = True
        step = self.get_step(step_id)
        if step.required:
            result = self.can_be_processed(step_id)

        return result

    def process_step(self, step_id: str) -> str:
        """Kick off processing for the step with the provided id.

        Arguments:
            step_id: the id of the step that should be started
        """

        step_inputs = self.get_step_inputs(step_id)

        # if the inputs are not valid, ignore this step
        if not step_inputs.items_are_valid():
            raise Exception(
                f"Can't execute step '{step_id}': it does not have valid input set."
            )

        # get the output 'holder' objects, which we'll need to pass to the module
        step_outputs = self.get_step_outputs(step_id)
        # get the module object that holds the code that will do the processing
        step = self.get_step(step_id)

        job_id = self._processor.start(
            pipeline_id=self.pipeline.id,
            pipeline_name=self.pipeline.structure.pipeline_id,
            step_id=step_id,
            module=step.module,
            inputs=step_inputs,
            outputs=step_outputs,
        )
        return job_id

    def step_is_ready(self, step_id: str) -> bool:
        """Return whether the step with the provided id is ready to be processed.

        A ``True`` result means that all input fields are currently set with valid values.

        Arguments:
            step_id: the id of the step to check

        Returns:
            whether the step is ready (``True``) or not (``False``)
        """
        return self.get_step_inputs(step_id).items_are_valid()

    def step_is_finished(self, step_id: str) -> bool:
        """Return whether the step with the provided id has been processed successfully.

        A ``True`` result means that all output fields are currently set with valid values, and the inputs haven't changed
        since the last time processing was done.

        Arguments:
            step_id: the id of the step to check

        Returns:
              whether the step result is valid (``True``) or not (``False``)
        """

        return self.get_step_outputs(step_id).items_are_valid()

    def pipeline_is_ready(self) -> bool:
        """Return whether the pipeline is ready to be processed.

        A ``True`` result means that all pipeline inputs are set with valid values, and therefore every step within the
        pipeline can be processed.

        Returns:
            whether the pipeline can be processed as a whole (``True``) or not (``False``)
        """

        return self.pipeline.inputs.items_are_valid()

    def pipeline_is_finished(self) -> bool:
        """Return whether the pipeline has been processed successfully.

        A ``True`` result means that every step of the pipeline has been processed successfully, and no pipeline input
        has changed since that happened.

        Returns:
            whether the pipeline was processed successfully (``True``) or not (``False``)
        """
        return self.pipeline.outputs.items_are_valid()

    def set_pipeline_inputs(self, **inputs: typing.Any):
        """Set one, several or all inputs for this pipeline.

        Arguments:
            **inputs: the input values to set
        """

        _inputs = self._pipeline_input_hook(**inputs)
        self.pipeline_inputs.set_values(**_inputs)

    def _pipeline_input_hook(self, **inputs: typing.Any):
        """Hook before setting input.

        Can be implemented by child controller classes, to prevent, transform, validate or queue inputs.
        """

        log.debug(f"Inputs for pipeline '{self.pipeline.id}' set: {inputs}")
        return inputs


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
        processor: typing.Optional[ModuleProcessor] = None,
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

    def process_pipeline(self) -> bool:

        if self._is_running:
            log.debug("Pipeline running, doing nothing.")
            return False

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
                                f"Required pipeline step '{step_id}' can't be processed, inputs not ready yet."
                            )
                    try:
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

        return True

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
