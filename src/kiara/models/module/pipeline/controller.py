# -*- coding: utf-8 -*-
import orjson.orjson
from typing import Hashable

from kiara.models.events.pipeline import (
    PipelineInputEvent,
    PipelineOutputEvent,
    StepInputEvent,
    StepOutputEvent,
)
from kiara.models.module.pipeline.pipeline import BasePipelineListener, Pipeline


class PipelineController(BasePipelineListener):

    pass


class SinglePipelineController(PipelineController):
    def __init__(self, pipeline: Pipeline):

        self._pipeline: Pipeline = pipeline

    def get_listener_id(self) -> Hashable:
        return self._pipeline.id

    def step_inputs_changed(self, event: StepInputEvent):
        """Method to override if the implementing controller needs to react to events where one or several step inputs have changed.

        Arguments:
            event: the step input event
        """

        print(f"STEP INPUTS: {event.json(option=orjson.orjson.OPT_INDENT_2)}")

    def step_outputs_changed(self, event: StepOutputEvent):
        """Method to override if the implementing controller needs to react to events where one or several step outputs have changed.

        Arguments:
            event: the step output event
        """

    def pipeline_inputs_changed(self, event: PipelineInputEvent):
        """Method to override if the implementing controller needs to react to events where one or several pipeline inputs have changed.

        !!! note
        Whenever pipeline inputs change, the connected step inputs also change and an (extra) event will be fired for those. Which means
        you can choose to only implement the ``step_inputs_changed`` method if you want to. This behaviour might change in the future.

        Arguments:
            event: the pipeline input event
        """

        print(f"PIPELINE INPUTS: {event.json(option=orjson.OPT_INDENT_2)}")

    def pipeline_outputs_changed(self, event: PipelineOutputEvent):
        """Method to override if the implementing controller needs to react to events where one or several pipeline outputs have changed.

        Arguments:
            event: the pipeline output event
        """
