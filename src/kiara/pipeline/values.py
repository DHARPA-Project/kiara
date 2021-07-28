# -*- coding: utf-8 -*-
import abc
import typing
import uuid
from datetime import datetime
from pydantic import BaseModel, Extra, Field, PrivateAttr, root_validator

from kiara.data.values import Value, ValueSchema
from kiara.defaults import PIPELINE_PARENT_MARKER
from kiara.pipeline.utils import generate_step_alias
from kiara.utils import camel_case_to_snake_case

if typing.TYPE_CHECKING:
    from kiara.data.registry import DataRegistry

try:

    class ValueUpdateHandler(typing.Protocol):
        """The call signature for callbacks that can be registered as value update handlers."""

        def __call__(self, *items: "Value", **kwargs: typing.Any) -> typing.Any:
            ...


except Exception:
    # there is some issue with older Python only_latest, typing.Protocol, and Pydantic
    ValueUpdateHandler = typing.Callable  # type:ignore


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


class KiaraValue(Value, abc.ABC):
    """A pointer to 'actual' data (bytes), along with metadata associated with this data.

    This object is created by a [DataRegistry][kiara.data.registry.DataRegistry], and can be used to retrieve the associated data
    from that registry. In addition, it can be used to subscribe to change events for that data, using the [register_callback][kiara.data.registry.DataRegistry.register_callback] method.
    The reason the data itself is not contained within this model is that the data could be very big,
    and it might not be necessary to hold them in memory in a lot of cases.
    """

    value_fields: typing.Tuple["ValueField", ...] = Field(
        description="Value fields within a pipeline connected to this value.",
        default_factory=set,
    )

    def __init__(self, **data):  # type: ignore
        data["stage"] = "init"

        super().__init__(**data)

    @property
    def registry(self) -> "DataRegistry":
        if self._kiara is None:
            raise Exception(f"Kiara object not set for value: {self}")
        return self._kiara.data_registry

    def register_callback(
        self, callback: typing.Callable
    ):  # this needs to implement ValueUpdateHandler, but can't add that type hint due to a pydantic error
        assert self._kiara is not None
        self._kiara.data_registry.register_callback(callback, self)

    def get_value_data(self) -> typing.Any:
        return self.registry.get_value_data(self)

    # def get_value_hash(self) -> typing.Any:
    #
    #     if self.value_hash == ValueHashMarker.DEFERRED:
    #         return self.registry.get_value_hash(self)
    #     else:
    #         return self.value_hash

    def __eq__(self, other):

        # TODO: compare all attributes if id is equal, just to make sure...

        if not isinstance(other, KiaraValue):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(id={str(self.id)} valid={self.item_is_valid()})"
        )

    def __str__(self):
        return self.__repr__()


class DataValue(KiaraValue):
    """An implementation of [Value][kiara.data.values.Value] that points to 'actual' data.

    This is opposed to a [LinkedValue][kiara.data.values.LinkedValue], which points to one or several other ``Value``
    objects, and is read-only.
    """

    @root_validator(pre=True)
    def validate_input_fields(cls, values):

        # TODO: validate against schema?

        if values.get("last_update", None):
            raise ValueError(
                "Can't set 'last_update', this value will be set automatically."
            )

        is_init = True if values.pop("stage", None) == "init" else False

        value_schema: ValueSchema = values.get("value_schema", None)
        if value_schema is not None and not is_init:
            raise ValueError(
                "Can't set value_schema after initial construction of a Value object."
            )
        else:
            if not isinstance(value_schema, ValueSchema):
                raise TypeError(f"Invalid type for ValueSchema: {type(value_schema)}")

        value_id: str = values.get("id", None)
        if value_id and not is_init:
            raise ValueError(
                "Can't set value id after initial construction of a Value object."
            )
        else:
            if not isinstance(value_id, str):
                raise TypeError(f"Invalid type for value id: {type(value_id)}")

        is_constant: bool = values.get("is_constant", None)
        if is_constant and not is_init:
            raise ValueError(
                "Can't set 'is_constant' value after initial construction of a Value object."
            )
        else:
            if not isinstance(is_constant, bool):
                raise TypeError(f"Invalid type for 'is_constant': {type(is_constant)}")

        values["last_update"] = datetime.now()

        return values

    def set_value_data(self, value: typing.Any) -> bool:

        # TODO: validate against schema
        changed: bool = self.registry.set_value(self, value)
        if changed:
            self._hash_cache = None
        return changed


class LinkedValue(KiaraValue):
    """An implementation of [Value][kiara.data.values.Value] that points to one or several other ``Value`` objects..

    This is opposed to a [DataValue][kiara.data.values.DataValue], which points to 'actual' data, and is read/write-able.
    """

    links: typing.Dict[str, typing.Dict[str, typing.Any]]

    @root_validator(pre=True)
    def validate_input_fields(cls, values):

        # TODO: validate against schema?

        if values.get("last_update", None):
            raise ValueError(
                "Can't set 'last_update', this value will be set automatically."
            )

        is_init = True if values.pop("stage", None) == "init" else False

        value_schema: ValueSchema = values.get("value_schema", None)
        if value_schema is not None and not is_init:
            raise ValueError(
                "Can't set value_schema after initial construction of a Value object."
            )
        else:
            if not isinstance(value_schema, ValueSchema):
                raise TypeError(f"Invalid type for ValueSchema: {type(value_schema)}")

        value_id: str = values.get("id", None)
        if value_id and not is_init:
            raise ValueError(
                "Can't set value id after initial construction of a Value object."
            )
        else:
            if not isinstance(value_id, str):
                raise TypeError(f"Invalid type for value id: {type(value_id)}")

        is_constant: bool = values.get("is_constant", None)
        if is_constant is not None:
            raise ValueError("Can't set 'is_constant' value in LinkedValue object.")

        values["last_update"] = datetime.now()
        values["is_constant"] = False

        return values

    def set_value_data(self, value: typing.Any) -> bool:
        raise Exception("Linked values can't be set.")


class ValueField(BaseModel):
    """An object that holds information about the location of a value within a pipeline.

    This object does not contain the value itself.

    There are four different ValuePoint types:

    - [kiara.data.values.StepInputField][]: an input to a step
    - [kiara.data.values.StepOutputField][]: an output of a step
    - [kiara.data.values.PipelineInputField][]: an input to a pipeline
    - [kiara.data.values.PipelineOutputField][]: an output for a pipeline

    Several point objects can target the same value, for example a step output and a connected step input are
    actually the same.
    """

    class Config:
        allow_mutation = True
        extra = Extra.forbid

    _id: uuid.UUID = PrivateAttr(default_factory=uuid.uuid4)
    value_name: str
    value_schema: ValueSchema
    pipeline_id: str

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
        return f"{self.__class__.__name__}(value_name='{self.value_name}' pipeline_id='{self.pipeline_id}'{step_id})"

    def __str__(self):
        name = camel_case_to_snake_case(self.__class__.__name__[0:-5], repl=" ")
        return f"{name}: {self.value_name} ({self.value_schema.type})"


class StepInputField(ValueField):
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


class StepOutputField(ValueField):
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


class PipelineInputField(ValueField):
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


class PipelineOutputField(ValueField):
    """An output to a pipeline."""

    connected_output: StepValueAddress = Field(description="Connected step outputs.")

    @property
    def alias(self) -> str:
        return generate_step_alias(PIPELINE_PARENT_MARKER, self.value_name)


DataValue.update_forward_refs()
LinkedValue.update_forward_refs()
StepInputField.update_forward_refs()
StepOutputField.update_forward_refs()
