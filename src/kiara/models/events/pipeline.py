# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from typing import Mapping

from kiara.models.events import ChangedValue, KiaraEvent

try:
    from typing import Literal
except Exception:
    from typing_extensions import Literal  # type: ignore

from pydantic import Field


class PipelineEvent(KiaraEvent):
    class Config:
        allow_mutation = False

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context that created the pipeline."
    )
    pipeline_id: uuid.UUID = Field(description="The pipeline id.")

    def __repr__(self):
        d = self.dict()
        d.pop("pipeline_id")
        return f"{self.__class__.__name__}(pipeline_id={self.pipeline_id} data={d}"

    def __str__(self):
        return self.__repr__()


class StepInputEvent(PipelineEvent):
    """Event that gets fired when one or several inputs for steps within a pipeline have changed."""

    event_type: Literal["step_input"] = "step_input"
    step_id: str = Field(description="The step id.")
    changed_inputs: Mapping[str, ChangedValue] = Field(
        description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
    )


class StepOutputEvent(PipelineEvent):
    """Event that gets fired when one or several outputs for steps within a pipeline have changed."""

    event_type: Literal["step_output"] = "step_output"

    step_id: str = Field(description="The step id.")
    changed_outputs: Mapping[str, ChangedValue] = Field(
        description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
    )


class PipelineInputEvent(PipelineEvent):
    """Event that gets fired when one or several inputs for the pipeline itself have changed."""

    event_type: Literal["pipeline_input"] = "pipeline_input"

    changed_inputs: Mapping[str, ChangedValue] = Field(
        description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
    )


class PipelineOutputEvent(PipelineEvent):
    """Event that gets fired when one or several outputs for the pipeline itself have changed."""

    event_type: Literal["pipeline_output"] = "pipeline_output"
    changed_outputs: Mapping[str, ChangedValue] = Field(
        description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
    )
