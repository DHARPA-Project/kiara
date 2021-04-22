# -*- coding: utf-8 -*-

"""A module that contains value-related classes for *Kiara*.

A value in Kiara-speak is a pointer to actual data (aka 'bytes'). It contains metadata about that data (like whether it's
valid/set, what type/schema it has, when it was last modified, ...), but it does not contain the data itself. The reason for
that is that such data can be fairly large, and in a lot of cases it is not necessary for the code involved to have
access to it, access to the metadata is enough.

Each Value has a unique id, which can be used to retrieve the data (whole, or parts of it) from a [DataRegistry][kiara.data.registry.DataRegistry]. In addition, that id can be used to subscribe to change events for a value (published
whenever the data that is associated with a value was changed).
"""

import abc
import json
import logging
import typing
import uuid
from datetime import datetime
from pydantic import BaseModel, Extra, Field, PrivateAttr, root_validator
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.syntax import Syntax
from rich.table import Table

from kiara.data.types import ValueType
from kiara.defaults import INVALID_VALUE_NAMES, PIPELINE_PARENT_MARKER
from kiara.utils import StringYAML, camel_case_to_snake_case

if typing.TYPE_CHECKING:
    from kiara.data.registry import DataRegistry
    from kiara.kiara import Kiara

log = logging.getLogger("kiara")
yaml = StringYAML()

try:

    class ValueUpdateHandler(typing.Protocol):
        """The call signature for callbacks that can be registered as value update handlers."""

        def __call__(self, *items: "Value", **kwargs: typing.Any) -> typing.Any:
            ...


except Exception:
    # there is some issue with older Python versions, typing.Protocol, and Pydantic
    ValueUpdateHandler = typing.Callable  # type:ignore


class StepValueAddress(BaseModel):
    """Small model to describe the address of a value of a step, within a Pipeline/PipelineStructure."""

    class Config:
        extra = Extra.forbid

    step_id: str = Field(description="The id of a step within a pipeline.")
    value_name: str = Field(
        description="The name of the value (output name or pipeline input name)."
    )
    sub_value: typing.Optional[str] = Field(
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


class ValueSchema(BaseModel):
    """The schema of a value.

    The schema contains the [ValueType][kiara.data.values.ValueType] of a value, as well as an optional default that
    will be used if no user input was given (yet) for a value.

    For more complex container types like arrays, tables, unions etc, types can also be configured with values from the ``type_config`` field.
    """

    class Config:
        use_enum_values = True
        extra = Extra.forbid

    type: str = Field(description="The type of the value.")
    type_config: typing.Dict[str, typing.Any] = Field(
        description="Configuration for the type, in case it's complex.",
        default_factory=dict,
    )

    doc: str = Field(
        default="-- n/a --",
        description="A description for the value of this input field.",
    )
    default: typing.Any = Field(description="A default value.", default=None)

    def validate_types(self, kiara: "Kiara"):

        if self.type not in kiara.value_type_names:
            raise ValueError(
                f"Invalid value type '{self.type}', available types: {kiara.value_type_names}"
            )

    def __eq__(self, other):

        if not isinstance(other, ValueSchema):
            return False

        return (self.type, self.default) == (other.type, other.default)

    def __hash__(self):

        return hash((self.type, self.default))


class Value(BaseModel, abc.ABC):
    """A pointer to 'actual' data (bytes), along with metadata associated with this data.

    This object is created by a [DataRegistry][kiara.data.registry.DataRegistry], and can be used to retrieve the associated data
    from that registry. In addition, it can be used to subscribe to change events for that data, using the [register_callback][kiara.data.registry.DataRegistry.register_callback] method.
    The reason the data itself is not contained within this model is that the data could be very big,
    and it might not be necessary to hold them in memory in a lot of cases.
    """

    class Config:
        extra = Extra.forbid
        use_enum_values = True

    _kiara: typing.Optional["Kiara"] = PrivateAttr()
    _type_obj: ValueType = PrivateAttr(default=None)

    id: str = Field(description="A unique id for this value.")
    value_schema: ValueSchema = Field(description="The schema of this value.")
    value_fields: typing.Tuple["ValueField", ...] = Field(
        description="Value fields within a pipeline connected to this value.",
        default_factory=set,
    )
    is_constant: bool = Field(
        description="Whether this value is a constant.", default=False
    )
    origin: typing.Optional[str] = Field(
        description="Description of how/where the value was set.", default="n/a"
    )
    last_update: typing.Optional[datetime] = Field(
        default=None, description="The time the last update to this value happened."
    )
    is_streaming: bool = Field(
        default=False,
        description="Whether the value is currently streamed into this object.",
    )
    is_valid: bool = Field(
        description="Whether the value is set and valid.", default=False
    )
    metadata: typing.Dict[str, typing.Any] = Field(
        description="Metadata relating to the actual data (size, no. of rows, etc. -- depending on data type).",
        default_factory=dict,
    )

    def __init__(self, **data):  # type: ignore
        data["stage"] = "init"
        kiara = data.pop("kiara", None)
        if kiara is None:
            raise ValueError("No 'kiara' object provided.")
        super().__init__(**data)
        self._kiara = kiara

    def get_value_data(self) -> typing.Any:
        """Retrieve the actual data from the registry.

        This will be implemented by subclasses.
        """
        raise NotImplementedError()

    @property
    def type_obj(self):
        if self._type_obj is None:
            cls = self._kiara.get_value_type_cls(self.value_schema.type)
            self._type_obj = cls(**self.value_schema.type_config)
        return self._type_obj

    @property
    def registry(self) -> "DataRegistry":
        if self._kiara is None:
            raise Exception(f"Kiara object not set for value: {self}")
        return self._kiara.data_registry

    def register_callback(
        self, callback: typing.Callable
    ):  # this needs to implement ValueUpdateHandler, but can't add that type hint due to a pydantic error
        self._kiara.data_registry.register_callback(callback, self)

    def __eq__(self, other):

        # TODO: compare all attributes if id is equal, just to make sure...

        if not isinstance(other, Value):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return f"{self.__class__.__name__}(id={str(self.id)} valid={self.is_valid})"

    def __str__(self):
        return self.__repr__()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield f"[b]Value: {self.origin}"

        table = Table(box=box.SIMPLE, show_header=False)
        table.add_column("property", style="i")
        table.add_column("value")

        table.add_row("id", self.id)
        table.add_row("type", self.value_schema.type)
        table.add_row("desc", self.value_schema.doc)
        table.add_row("is set", "yes" if self.is_valid else "no")
        table.add_row("is constant", "yes" if self.is_constant else "no")

        if self.metadata:
            json_string = json.dumps(self.metadata, indent=2)
            metadata = Syntax(json_string, "json")
            table.add_row("metadata", metadata)
        else:
            table.add_row("metadata", "-- no metadata --")

        yield table


class DataValue(Value):
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

        if is_constant:
            values["origin"] = "constant"

        values["last_update"] = datetime.now()

        return values

    def get_value_data(self) -> typing.Any:
        return self.registry.get_value_data(self)

    def set_value_data(self, value: typing.Any) -> bool:

        # TODO: validate against schema
        changed: bool = self.registry.set_value(self, value)
        return changed


class LinkedValue(Value):
    """An implementation of [Value][kiara.data.values.Value] that points to one or several other ``Value`` objects..

    This is opposed to a [DataValue][kiara.data.values.DataValue], which points to 'actual' data, and is read/write-able.
    """

    links: typing.Dict[str, typing.Dict[str, str]]

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

    def get_value_data(self) -> typing.Any:
        return self.registry.get_value_data(self)

    def set_value_data(self, value: typing.Any) -> bool:
        raise Exception("Linked values can't be set.")


class ValueSet(typing.MutableMapping[str, Value]):
    """A dict-like object that contains a set of value fields that belong together in some way (for example outputs of a step or pipeline)."""

    def __init__(
        self, items: typing.Mapping[str, Value], title: typing.Optional[str] = None
    ):

        if not items:
            raise ValueError("Can't create ValueSet: no values provided")

        for item, value in items.items():

            if value is None:
                raise Exception(
                    f"Can't create value set, item '{item}' does not have a value (yet)."
                )

            if item.startswith("_"):
                raise ValueError(f"Value name can't start with '_': {item}")
            if item in INVALID_VALUE_NAMES:
                raise ValueError(f"Invalid value name '{item}'.")
        super(ValueSet, self).__setattr__("_value_items", items)
        # TODO: auto-generate doc
        # TODO: auto-generate schema
        # TODO: validate schema types
        schema = ValueSchema(type="any", default=None, doc="-- n/a --")
        self._schema = schema
        if title is None:
            title = "-- n/a --"
        self._title = title

    def __getattr__(self, item):

        # if item == "ALL":
        if item == "_value_items":
            raise KeyError()

        # if item == "ALL":
        #     return {k: v. for k, v in self.__dict__["_value_items"].items()}
        elif item in self.__dict__["_value_items"].keys():
            return self.__dict__["_value_items"][item]
        else:
            try:
                return super().__getattribute__(item)
            except AttributeError:
                raise AttributeError(
                    f"ValueSet does not have a field '{item}', available fields: {' ,'.join(self._value_items.keys())}"
                )

    def __setattr__(self, key, value):

        if key == "ALL":
            self.set_values(**value)
        elif key in self._value_items.keys():
            self.set_values(**{key: value})
        elif key.startswith("_") or key in INVALID_VALUE_NAMES:
            self.__dict__[key] = value
        else:
            av = list(self._value_items.keys())
            raise Exception(
                f"Can't set value, invalid field name '{key}'. Available fields: {', '.join(av)}"
            )

    def __getitem__(self, item: str) -> Value:

        return self._value_items[item]

    def __setitem__(self, key: str, value: Value):

        self.set_values(**{key: value})

    def __delitem__(self, key: str):

        raise Exception(f"Removing items not supported: {key}")

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._value_items)

    def __len__(self):
        return len(self._value_items)

    @property
    def items_are_valid(self) -> bool:

        for item in self._value_items.values():
            if item is None or not item.is_valid:
                return False
        return True

    def dict(self):
        result = {}
        for k, v in self._value_items.items():
            result[k] = v.get_value_data()
        return result

    def set_values(self, **values: typing.Any) -> typing.Dict[Value, bool]:

        invalid: typing.List[str] = []
        registries: typing.Dict[DataRegistry, typing.Dict[Value, typing.Any]] = {}

        for k, v in values.items():

            if isinstance(v, Value):
                raise Exception("Invalid value type")

            if k not in self._value_items.keys():
                invalid.append(k)
            else:
                item: Value = self._value_items[k]
                registries.setdefault(item.registry, {})[item] = v

        if invalid:
            raise ValueError(
                f"No value item(s) with name(s) {', '.join(invalid)} available, valid names: {', '.join(self._value_items.keys())}"
            )

        result: typing.Dict[Value, bool] = {}

        for registry, v in registries.items():
            _r = registry.set_values(v)
            result.update(_r)

        return result

    def to_details(self) -> "PipelineValues":

        result = {}
        for name, item in self._value_items.items():
            result[name] = PipelineValue.from_value_obj(item)

        return PipelineValues(values=result)

    def to_dict(self) -> typing.Dict[str, typing.Any]:

        return self.to_details().dict()

    def to_json(self) -> str:

        return self.to_details().json()

    def __repr__(self):

        return f"ValueItems(values={self._value_items} valid={self.items_are_valid})"

    def _create_rich_table(self, show_headers: bool = True) -> Table:

        table = Table(box=box.SIMPLE, show_header=show_headers)
        table.add_column("name", style="i")
        table.add_column("type")
        table.add_column("desc")
        table.add_column("is set")

        for k, v in self.items():
            t = v.value_schema.type
            desc = v.value_schema.doc
            is_set = "yes" if v.is_valid else "no"
            table.add_row(k, t, desc, is_set)

        return table

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield f"[b]Value-set: {self._title}[/b]"
        yield self._create_rich_table(show_headers=True)


ValueSchema.update_forward_refs()


class PipelineValue(BaseModel):
    """Convenience wrapper to make the [PipelineState][kiara.pipeline.pipeline.PipelineState] json/dict export prettier."""

    @classmethod
    def from_value_obj(cls, value: Value):

        return PipelineValue(
            id=value.id,
            value_schema=value.value_schema,
            is_valid=value.is_valid,
            is_constant=value.is_constant,
            origin=value.origin,
            last_update=value.last_update,
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
    value_schema: ValueSchema = Field(description="The schema of this value.")
    is_constant: bool = Field(
        description="Whether this value is a constant.", default=False
    )
    origin: typing.Optional[str] = Field(
        description="Description of how/where the value was set.", default="n/a"
    )
    last_update: datetime = Field(
        default=None, description="The time the last update to this value happened."
    )
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
    def from_value_set(cls, value_set: ValueSet):

        values: typing.Dict[str, PipelineValue] = {}
        for k, v in value_set.items():
            values[k] = PipelineValue.from_value_obj(v)

        return PipelineValues(values=values)

    values: typing.Dict[str, PipelineValue] = Field(
        description="Field names are keys, and the data as values."
    )

    class Config:
        use_enum_values = True


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
        allow_mutation = False
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


def generate_step_alias(step_id: str, value_name):
    return f"{step_id}.{value_name}"


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

    @property
    def alias(self) -> str:
        return generate_step_alias(PIPELINE_PARENT_MARKER, self.value_name)


class PipelineOutputField(ValueField):
    """An output to a pipeline."""

    connected_output: StepValueAddress = Field(description="Connected step outputs.")

    @property
    def alias(self) -> str:
        return generate_step_alias(PIPELINE_PARENT_MARKER, self.value_name)


Value.update_forward_refs()
DataValue.update_forward_refs()
LinkedValue.update_forward_refs()
StepInputField.update_forward_refs()
StepOutputField.update_forward_refs()
