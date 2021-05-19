# -*- coding: utf-8 -*-
import abc

from kiara.events import (
    PipelineInputEvent,
    PipelineOutputEvent,
    StepInputEvent,
    StepOutputEvent,
)


class PipelineListener(abc.ABC):
    def step_inputs_changed(self, event: StepInputEvent):
        """Method to override if the implementing controller needs to react to events where one or several step inputs have changed.

        Arguments:
            event: the step input event
        """

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

    def pipeline_outputs_changed(self, event: PipelineOutputEvent):
        """Method to override if the implementing controller needs to react to events where one or several pipeline outputs have changed.

        Arguments:
            event: the pipeline output event
        """


# class DebugListener(PipelineListener):
#     def step_inputs_changed(self, event: StepInputEvent):
#
#         pp(event.dict())
#
#     def step_outputs_changed(self, event: StepOutputEvent):
#
#         pp(event.dict())
#
#     def pipeline_inputs_changed(self, event: PipelineInputEvent):
#
#         pp(event.dict())
#
#     def pipeline_outputs_changed(self, event: PipelineOutputEvent):
#
#         pp(event.dict())
