# -*- coding: utf-8 -*-
import typing
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Extra, Field

from kiara.data import Value, ValueSet
from kiara.data.values import ValueMetadata, ValueSchema
from kiara.pipeline.utils import generate_step_alias


class PipelineValue(BaseModel):
    """Convenience wrapper to make the [PipelineState][kiara.pipeline.pipeline.PipelineState] json/dict export prettier."""

    @classmethod
    def from_value_obj(cls, value: Value, ensure_metadata: bool = False):

        if ensure_metadata:
            value.get_metadata()

        return PipelineValue(
            id=value.id,
            value_schema=value.value_schema,
            is_valid=value.item_is_valid(),
            is_set=value.is_set,
            is_constant=value.is_constant,
            value_metadata=value.value_metadata,
            last_update=value.last_update,
            # value_hash=value.value_hash,
            is_streaming=value.is_streaming,
            metadata=value.metadata,
        )

    class Config:
        extra = Extra.forbid
        allow_mutation = False

    id: str = Field(description="A unique id for this value.")
    is_valid: bool = Field(
        description="Whether the value is set and valid.", default=False
    )
    is_set: bool = Field(description="Whether the value is set.")
    value_schema: ValueSchema = Field(description="The schema of this value.")
    is_constant: bool = Field(
        description="Whether this value is a constant.", default=False
    )
    value_metadata: ValueMetadata = Field(
        description="The metadata of the value itself (not the actual data)."
    )
    last_update: datetime = Field(
        default=None, description="The time the last update to this value happened."
    )
    # value_hash: typing.Union[ValueHashMarker, int] = Field(
    #     description="The hash of the current value."
    # )
    is_streaming: bool = Field(
        default=False,
        description="Whether the value is currently streamed into this object.",
    )
    metadata: typing.Dict[str, typing.Any] = Field(
        description="Metadata relating to the actual data (size, no. of rows, etc. -- depending on data type).",
        default_factory=dict,
    )


class PipelineValues(BaseModel):
    """Convenience wrapper to make the [PipelineState][kiara.pipeline.pipeline.PipelineState] json/dict export prettier.

    This is basically just a simplified version of the [ValueSet][kiara.data.values.ValueSet] class that is using
    pydantic, in order to make it easy to export to json.
    """

    @classmethod
    def from_value_set(cls, value_set: ValueSet, ensure_metadata: bool = False):

        from kiara.pipeline.values import KiaraValue

        values: typing.Dict[str, PipelineValue] = {}
        for k in value_set.get_all_field_names():
            v = value_set.get_value_obj(k, ensure_metadata=ensure_metadata)
            if not isinstance(v, KiaraValue):
                raise TypeError(f"Invalid type of value: {type(v)}")
            values[k] = PipelineValue.from_value_obj(v, ensure_metadata=ensure_metadata)

        return PipelineValues(values=values)

    values: typing.Dict[str, PipelineValue] = Field(
        description="Field names are keys, and the data as values."
    )

    class Config:
        use_enum_values = True


class StepValueAddress(BaseModel):
    """Small model to describe the address of a value of a step, within a Pipeline/PipelineStructure."""

    class Config:
        extra = Extra.forbid

    step_id: str = Field(description="The id of a step within a pipeline.")
    value_name: str = Field(
        description="The name of the value (output name or pipeline input name)."
    )
    sub_value: typing.Optional[typing.Dict[str, typing.Any]] = Field(
        default=None,
        description="A reference to a subitem of a value (e.g. column, list item)",
    )

    @property
    def alias(self):
        """An alias string for this address (in the form ``[step_id].[value_name]``)."""
        return generate_step_alias(self.step_id, self.value_name)

    def __eq__(self, other):

        if not isinstance(other, StepValueAddress):
            return False

        return (self.step_id, self.value_name, self.sub_value) == (
            other.step_id,
            other.value_name,
            other.sub_value,
        )

    def __hash__(self):

        return hash((self.step_id, self.value_name, self.sub_value))

    def __repr__(self):

        if self.sub_value:
            sub_value = f" sub_value={self.sub_value}"
        else:
            sub_value = ""
        return f"StepValueAddres(step_id={self.step_id}, value_name={self.value_name}{sub_value})"

    def __str__(self):
        return self.__repr__()


class StepStatus(Enum):
    """Enum to describe the state of a workflow."""

    STALE = "stale"
    INPUTS_READY = "inputs_ready"
    RESULTS_INCOMING = "processing"
    RESULTS_READY = "results_ready"
