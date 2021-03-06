# -*- coding: utf-8 -*-
import typing
import uuid
from pydantic import BaseModel, Extra, Field, PrivateAttr, root_validator

from kiara.data.values import ValueSchema
from kiara.defaults import PIPELINE_PARENT_MARKER
from kiara.pipeline import StepValueAddress
from kiara.pipeline.utils import generate_step_alias
from kiara.utils import camel_case_to_snake_case


class ValueRef(BaseModel):
    """An object that holds information about the location of a value within a pipeline (or other structure).

    Basically, a `ValueRef` helps the containing object where in its structure the value belongs (for example so
    it can update dependent other values). A `ValueRef` object (obviously) does not contain the value itself.

    There are four different ValueRef type that are relevant for pipelines:

    - [kiara.pipeline.values.StepInputRef][]: an input to a step
    - [kiara.pipeline.values.StepOutputRef][]: an output of a step
    - [kiara.pipeline.values.PipelineInputRef][]: an input to a pipeline
    - [kiara.pipeline.values.PipelineOutputRef][]: an output for a pipeline

    Several `ValueRef` objects can target the same value, for example a step output and a connected step input would
    reference the same `Value` (in most cases)..
    """

    class Config:
        allow_mutation = True
        extra = Extra.forbid

    _id: uuid.UUID = PrivateAttr(default_factory=uuid.uuid4)
    value_name: str
    value_schema: ValueSchema
    # pipeline_id: str

    def __eq__(self, other):

        if not isinstance(other, self.__class__):
            return False

        return self._id == other._id

    def __hash__(self):
        return hash(self._id)

    def __repr__(self):
        step_id = ""
        if hasattr(self, "step_id"):
            step_id = f" step_id='{self.step_id}'"
        return f"{self.__class__.__name__}(value_name='{self.value_name}' {step_id})"

    def __str__(self):
        name = camel_case_to_snake_case(self.__class__.__name__[0:-5], repl=" ")
        return f"{name}: {self.value_name} ({self.value_schema.type})"


class StepInputRef(ValueRef):
    """An input to a step.

    This object can either have a 'connected_outputs' set, or a 'connected_pipeline_input', not both.
    """

    step_id: str = Field(description="The step id.")
    connected_outputs: typing.Optional[typing.List[StepValueAddress]] = Field(
        default=None,
        description="A potential connected list of one or several module outputs.",
    )
    connected_pipeline_input: typing.Optional[str] = Field(
        default=None, description="A potential pipeline input."
    )
    is_constant: bool = Field(
        "Whether this input is a constant and can't be changed by the user."
    )

    @root_validator(pre=True)
    def ensure_single_connected_item(cls, values):

        if values.get("connected_outputs", None) and values.get(
            "connected_pipeline_input"
        ):
            raise ValueError("Multiple connected items, only one allowed.")

        return values

    @property
    def alias(self) -> str:
        return generate_step_alias(self.step_id, self.value_name)

    @property
    def address(self) -> StepValueAddress:
        return StepValueAddress(step_id=self.step_id, value_name=self.value_name)

    def __str__(self):
        name = camel_case_to_snake_case(self.__class__.__name__[0:-5], repl=" ")
        return f"{name}: {self.step_id}.{self.value_name} ({self.value_schema.type})"


class StepOutputRef(ValueRef):
    """An output to a step."""

    class Config:
        allow_mutation = True

    step_id: str = Field(description="The step id.")
    pipeline_output: typing.Optional[str] = Field(
        description="The connected pipeline output."
    )
    connected_inputs: typing.List[StepValueAddress] = Field(
        description="The step inputs that are connected to this step output",
        default_factory=list,
    )

    @property
    def alias(self) -> str:
        return generate_step_alias(self.step_id, self.value_name)

    @property
    def address(self) -> StepValueAddress:
        return StepValueAddress(step_id=self.step_id, value_name=self.value_name)

    def __str__(self):
        name = camel_case_to_snake_case(self.__class__.__name__[0:-5], repl=" ")
        return f"{name}: {self.step_id}.{self.value_name} ({self.value_schema.type})"


class PipelineInputRef(ValueRef):
    """An input to a pipeline."""

    connected_inputs: typing.List[StepValueAddress] = Field(
        description="The step inputs that are connected to this pipeline input",
        default_factory=list,
    )
    is_constant: bool = Field(
        "Whether this input is a constant and can't be changed by the user."
    )

    @property
    def alias(self) -> str:
        return generate_step_alias(PIPELINE_PARENT_MARKER, self.value_name)


class PipelineOutputRef(ValueRef):
    """An output to a pipeline."""

    connected_output: StepValueAddress = Field(description="Connected step outputs.")

    @property
    def alias(self) -> str:
        return generate_step_alias(PIPELINE_PARENT_MARKER, self.value_name)


StepInputRef.update_forward_refs()
StepOutputRef.update_forward_refs()
