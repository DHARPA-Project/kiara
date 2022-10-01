# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
import uuid
from sortedcontainers import SortedDict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, MutableMapping, Set, Union

from kiara.defaults import NONE_VALUE_ID, NOT_SET_VALUE_ID
from kiara.models import KiaraModel
from kiara.models.events import KiaraEvent
from kiara.models.module.pipeline import PipelineStep, StepStatus
from kiara.utils.json import orjson_dumps

try:
    from typing import Literal
except Exception:
    from typing_extensions import Literal  # type: ignore  # noqa

from pydantic import BaseModel, Field, validator

if TYPE_CHECKING:
    from kiara.models.module.pipeline.pipeline import Pipeline


class ChangedValue(BaseModel):

    old: Union[uuid.UUID, None]
    new: Union[uuid.UUID, None]


class StepDetails(BaseModel):

    kiara_id: uuid.UUID = Field(description="The id of the kiara context.")
    pipeline_id: uuid.UUID = Field(description="The id of the pipeline.")
    step: PipelineStep = Field(description="The pipeline step details.")
    step_id: str = Field(description="The id of the step.")
    processing_stage: int = Field(
        description="The execution stage where this step is executed."
    )
    status: StepStatus = Field(description="The current status of this step.")
    invalid_details: Dict[str, str] = Field(
        description="Details about fields that are invalid (if status < 'INPUTS_READY'.",
        default_factory=dict,
    )
    inputs: Dict[str, uuid.UUID] = Field(description="The current inputs of this step.")
    outputs: Dict[str, uuid.UUID] = Field(
        description="The current outputs of this step."
    )

    @validator("inputs")
    def replace_none_values_inputs(cls, value):

        result = {}
        for k, v in value.items():
            if v is None:
                v = NONE_VALUE_ID
            result[k] = v
        return result

    @validator("outputs")
    def replace_none_values_outputs(cls, value):

        result = {}
        for k, v in value.items():
            if v is None:
                v = NOT_SET_VALUE_ID
            result[k] = v
        return result

    def _retrieve_data_to_hash(self) -> Any:
        return f"{self.kiara_id}.{self.pipeline_id}.{self.step_id}"

    def _retrieve_id(self) -> str:
        return f"{self.kiara_id}.{self.pipeline_id}.{self.step_id}"


class PipelineState(KiaraModel):

    _kiara_model_id = "instance.pipeline_state"

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps

    kiara_id: uuid.UUID = Field(description="The id of the kiara context.")
    pipeline_id: uuid.UUID = Field(description="The id of the pipeline.")

    pipeline_status: StepStatus = Field(
        description="The current status of this pipeline."
    )
    invalid_details: Dict[str, str] = Field(
        description="Details about fields that are invalid (if status < 'INPUTS_READY'.",
        default_factory=dict,
    )

    pipeline_inputs: Dict[str, uuid.UUID] = Field(
        description="The current pipeline inputs."
    )
    pipeline_outputs: Dict[str, uuid.UUID] = Field(
        description="The current pipeline outputs."
    )

    step_states: Dict[str, StepDetails] = Field(
        description="The state of each step within this pipeline."
    )

    def get_steps_by_processing_stage(self) -> MutableMapping[int, List[StepDetails]]:

        result: MutableMapping[int, List[StepDetails]] = SortedDict()
        for step_details in self.step_states.values():
            result.setdefault(step_details.processing_stage, []).append(step_details)
        return result


class PipelineEvent(KiaraEvent):
    @classmethod
    def create_event(
        cls,
        pipeline: "Pipeline",
        changed: Mapping[str, Mapping[str, Mapping[str, ChangedValue]]],
    ) -> Union["PipelineEvent", None]:

        pipeline_inputs = changed.get("__pipeline__", {}).get("inputs", {})
        pipeline_outputs = changed.get("__pipeline__", {}).get("outputs", {})

        step_inputs = {}
        step_outputs = {}

        invalidated_steps: Set[str] = set()

        for step_id, change_details in changed.items():
            if step_id == "__pipeline__":
                continue
            inputs = change_details.get("inputs", None)
            if inputs:
                invalidated_steps.add(step_id)
                step_inputs[step_id] = inputs
            outputs = change_details.get("outputs", None)
            if outputs:
                invalidated_steps.add(step_id)
                step_outputs[step_id] = outputs

        if (
            not pipeline_inputs
            and not pipeline_outputs
            and not step_inputs
            and not step_outputs
            and not invalidated_steps
        ):
            return None

        event = PipelineEvent(
            kiara_id=pipeline.kiara_id,
            pipeline_id=pipeline.pipeline_id,
            pipeline_inputs_changed=pipeline_inputs,
            pipeline_outputs_changed=pipeline_outputs,
            step_inputs_changed=step_inputs,
            step_outputs_changed=step_outputs,
            changed_steps=sorted(invalidated_steps),
        )
        return event

    class Config:
        allow_mutation = False

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context that created the pipeline."
    )
    pipeline_id: uuid.UUID = Field(description="The pipeline id.")

    pipeline_inputs_changed: Dict[str, ChangedValue] = Field(
        description="Details about changed pipeline input values.", default_factory=dict
    )
    pipeline_outputs_changed: Dict[str, ChangedValue] = Field(
        description="Details about changed pipeline output values.",
        default_factory=dict,
    )

    step_inputs_changed: Dict[str, Mapping[str, ChangedValue]] = Field(
        description="Details about changed step input values.", default_factory=dict
    )
    step_outputs_changed: Dict[str, Mapping[str, ChangedValue]] = Field(
        description="Details about changed step output values.", default_factory=dict
    )

    changed_steps: List[str] = Field(
        description="A list of all step ids that have newly invalidated outputs."
    )

    def __repr__(self):
        return f"{self.__class__.__name__}(pipeline_id={self.pipeline_id}, invalidated_steps={', '.join(self.changed_steps)})"

    def __str__(self):
        return self.__repr__()


# class StepInputEvent(PipelineEvent):
#     """Event that gets fired when one or several inputs for steps within a pipeline have changed."""
#
#     event_type: Literal["step_input"] = "step_input"
#     step_id: str = Field(description="The step id.")
#     changed_inputs: Mapping[str, ChangedValue] = Field(
#         description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
#     )
#
#
#
# class StepOutputEvent(PipelineEvent):
#     """Event that gets fired when one or several outputs for steps within a pipeline have changed."""
#
#     event_type: Literal["step_output"] = "step_output"
#
#     step_id: str = Field(description="The step id.")
#     changed_outputs: Mapping[str, ChangedValue] = Field(
#         description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
#     )
#
#
# class PipelineInputEvent(PipelineEvent):
#     """Event that gets fired when one or several inputs for the pipeline itself have changed."""
#
#     event_type: Literal["pipeline_input"] = "pipeline_input"
#
#     changed_inputs: Mapping[str, ChangedValue] = Field(
#         description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
#     )
#
#
# class PipelineOutputEvent(PipelineEvent):
#     """Event that gets fired when one or several outputs for the pipeline itself have changed."""
#
#     event_type: Literal["pipeline_output"] = "pipeline_output"
#     changed_outputs: Mapping[str, ChangedValue] = Field(
#         description="steps (keys) with updated inputs which need re-processing (value is list of updated input names)"
#     )
