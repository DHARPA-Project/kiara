# -*- coding: utf-8 -*-
import typing
from datetime import datetime
from pydantic import BaseModel, Extra, Field

from kiara.data import Value, ValueSet
from kiara.data.values import ValueMetadata, ValueSchema
from kiara.pipeline.values import KiaraValue


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
