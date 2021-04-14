# -*- coding: utf-8 -*-
import typing
from pydantic import BaseModel, Field


class StepEvent(BaseModel):
    class Config:
        allow_mutation = False

    pipeline_id: str

    def __repr__(self):
        d = self.dict()
        d.pop("pipeline_id")
        return f"{self.__class__.__name__}(pipeline_id={self.pipeline_id} data={d}"

    def __str__(self):
        return self.__repr__()


class StepInputEvent(StepEvent):
    """Event that gets fired when one or several inputs for steps within a pipeline have changed."""

    updated_step_inputs: typing.Dict[str, typing.List[str]] = Field(
        description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
    )

    @property
    def newly_stale_steps(self) -> typing.List[str]:
        """Convenience method to display the steps that have been rendered 'stale' by this event."""
        return list(self.updated_step_inputs.keys())


class StepOutputEvent(StepEvent):
    """Event that gets fired when one or several outputs for steps within a pipeline have changed."""

    updated_step_outputs: typing.Dict[str, typing.List[str]] = Field(
        description="steps (keys) that finished processing of one, several or all outputs (values are list of 'finished' output fields)"
    )


class PipelineInputEvent(StepEvent):
    """Event that gets fired when one or several inputs for the pipeline itself have changed."""

    updated_pipeline_inputs: typing.List[str] = Field(
        description="list of pipeline input names that where changed"
    )


class PipelineOutputEvent(StepEvent):
    """Event that gets fired when one or several outputs for the pipeline itself have changed."""

    updated_pipeline_outputs: typing.List[str] = Field(
        description="list of pipeline output names that where changed"
    )


class OtherEvent(StepEvent):

    new_streaming_input: typing.Dict[str, typing.List[str]] = Field(
        description="steps (keys) where there was new data streamed to one or more inputs (values are list of those input names)"
    )
