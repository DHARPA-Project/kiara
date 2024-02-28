# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Set,
    Union,
)

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.table import Table
from sortedcontainers import SortedDict

from kiara.defaults import NONE_VALUE_ID, NOT_SET_VALUE_ID
from kiara.models import KiaraModel
from kiara.models.events import KiaraEvent
from kiara.models.module.pipeline import PipelineStep, StepStatus
from kiara.utils.output import create_renderable_from_value_id_map

if TYPE_CHECKING:
    from dag_cbor import IPLDKind

    from kiara.context import Kiara
    from kiara.models.module.pipeline.pipeline import Pipeline


class ChangedValue(BaseModel):

    old: Union[uuid.UUID, None] = None
    new: Union[uuid.UUID, None] = None


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
    _kiara: "Kiara" = PrivateAttr()

    @field_validator("inputs")
    @classmethod
    def replace_none_values_inputs(cls, value):

        result = {}
        for k, v in value.items():
            if v is None:
                v = NONE_VALUE_ID
            result[k] = v
        return result

    @field_validator("outputs")
    @classmethod
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

    def create_renderable(self, **config: Any) -> RenderableType:

        display_pipeline_id = config.get("display_pipeline_id", False)
        display_extended_step_details = config.get(
            "display_extended_step_details", False
        )

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")
        table.add_row("step id", self.step_id)
        table.add_row("status", self.status.value)
        if self.invalid_details:
            invalid_table = Table(show_header=False, box=box.SIMPLE)
            invalid_table.add_column("key", style="i")
            invalid_table.add_column("value")
            for k, v in self.invalid_details.items():
                invalid_table.add_row(k, v)
            table.add_row("invalid details", invalid_table)
        if display_pipeline_id:
            table.add_row("pipeline id", str(self.pipeline_id))
        table.add_row("processing stage", str(self.processing_stage))

        if display_extended_step_details:
            step_detail_config = dict(config)
            step_detail_config["display_step_id"] = False
            step_details = self.step.create_renderable(**step_detail_config)
            table.add_row("step details", step_details)

        inputs_rend = create_renderable_from_value_id_map(
            kiara=self._kiara, values=self.inputs, config=config
        )
        table.add_row("inputs", inputs_rend)
        outputs_rend = create_renderable_from_value_id_map(
            kiara=self._kiara, values=self.outputs, config=config
        )
        table.add_row("outputs", outputs_rend)

        return table


class PipelineState(KiaraModel):

    _kiara_model_id: ClassVar = "instance.pipeline_state"

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
    # pipeline_inputs_schema: Mapping[str, ValueSchema] = Field(description="The schema of the pipeline inputs.")
    pipeline_outputs: Dict[str, uuid.UUID] = Field(
        description="The current pipeline outputs."
    )
    # pipeline_outputs_schema: Mapping[str, ValueSchema] = Field(description="The schema of the pipeline outputs.")

    step_states: Dict[str, StepDetails] = Field(
        description="The state of each step within this pipeline."
    )
    _kiara: "Kiara" = PrivateAttr()

    def _retrieve_data_to_hash(self) -> "IPLDKind":
        """
        Return data important for hashing this model instance. Implemented by sub-classes.

        This returns the relevant data that makes this model unique, excluding any secondary metadata that is not
        necessary for this model to be used functionally. Like for example documentation.
        """

        # TODO: is this enough?
        return {
            "kiara_id": str(self.kiara_id),
            "pipeline_id": str(self.pipeline_id),
        }

    def get_steps_by_processing_stage(self) -> MutableMapping[int, List[StepDetails]]:

        result: MutableMapping[int, List[StepDetails]] = SortedDict()
        for step_details in self.step_states.values():
            result.setdefault(step_details.processing_stage, []).append(step_details)
        return result

    def get_processing_stage_status(self, stage: int) -> StepStatus:

        step_states = self.get_steps_by_processing_stage()

        status: StepStatus = StepStatus.RESULTS_READY

        for _stage, step_details in step_states.items():
            if _stage > stage:
                break

            for step in step_details:
                if step.status == StepStatus.INPUTS_INVALID:
                    status = StepStatus.INPUTS_INVALID
                    break

                elif step.status == StepStatus.INPUTS_READY:
                    if status != StepStatus.INPUTS_INVALID:
                        status = StepStatus.INPUTS_READY

            # no point in further checking
            if status == StepStatus.INPUTS_INVALID:
                return status

        return status

    def create_renderable(self, **config: Any) -> RenderableType:

        display_step_states = config.get("display_step_details", False)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")
        table.add_row("pipeline id", str(self.pipeline_id))
        table.add_row("pipeline status", self.pipeline_status.value)
        if self.invalid_details:
            invalid_table = Table(show_header=False, box=box.SIMPLE)
            invalid_table.add_column("key", style="i")
            invalid_table.add_column("value")
            for k, v in self.invalid_details.items():
                invalid_table.add_row(k, v)
            table.add_row("invalid details", invalid_table)

        render_conf = dict(config)
        render_conf["value_title"] = "field"
        render_conf["show_hash"] = False
        render_conf["show_size"] = False
        render_conf["show_data"] = True
        render_conf["max_lines"] = 5
        render_conf["display_extended_step_details"] = False
        render_conf["show_description"] = False

        inputs_rend = create_renderable_from_value_id_map(
            kiara=self._kiara, values=self.pipeline_inputs, config=render_conf
        )
        table.add_row("pipeline inputs", inputs_rend)

        step_details_table = Table(show_header=False, box=box.SIMPLE)
        step_details_table.add_column("step id")
        step_details_table.add_column("details")

        for (
            processing_stage,
            state_step_details,
        ) in self.get_steps_by_processing_stage().items():

            proc_status = self.get_processing_stage_status(processing_stage)
            proc_status_str = StepStatus.to_console_renderable(proc_status)
            step_details_table.add_row(
                f"processing stage: [b]{processing_stage}[/b]",
                f"[b i]{proc_status_str}[/b i]",
            )
            step_details_table.add_row("", "")

            if display_step_states:
                for step_details in state_step_details:
                    step_id = step_details.step_id
                    step_rend = step_details.create_renderable(**render_conf)
                    panel = Panel(step_rend)
                    step_details_table.add_row(f"step: [b i]{step_id}[/b i]", panel)
            else:
                for step_details in state_step_details:
                    step_id = step_details.step_id
                    step_status_str = StepStatus.to_console_renderable(
                        step_details.status
                    )
                    step_details_table.add_row(
                        f"step: [b i]{step_id}[/b i]", step_status_str
                    )

            step_details_table.add_row("", "")

        table.add_row("internal state", step_details_table)

        outputs_rend = create_renderable_from_value_id_map(
            kiara=self._kiara, values=self.pipeline_outputs, config=render_conf
        )
        table.add_row("pipeline outputs", outputs_rend)

        return table


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

    model_config = ConfigDict(frozen=True)

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
