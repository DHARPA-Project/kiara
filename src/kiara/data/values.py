# -*- coding: utf-8 -*-

"""A module that contains value-related classes for *Kiara*.

A value in Kiara-speak is a pointer to actual data (aka 'bytes'). It contains metadata about that data (like whether it's
valid/set, what type/schema it has, when it was last modified, ...), but it does not contain the data itself. The reason for
that is that such data can be fairly large, and in a lot of cases it is not necessary for the code involved to have
access to it, access to the metadata is enough.

Each Value has a unique id, which can be used to retrieve the data (whole, or parts of it) from a [DataRegistry][kiara.data.registry.DataRegistry].
In addition, that id can be used to subscribe to change events for a value (published whenever the data that is associated with a value was changed).
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
from rich.jupyter import JupyterMixin
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from kiara.data.types import ValueType
from kiara.defaults import INVALID_VALUE_NAMES, PIPELINE_PARENT_MARKER, SpecialValue
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


class ValueSchema(BaseModel):
    """The schema of a value.

    The schema contains the [ValueType][kiara.data.values.ValueType] of a value, as well as an optional default that
    will be used if no user input was given (yet) for a value.

    For more complex container types like array, tables, unions etc, types can also be configured with values from the ``type_config`` field.
    """

    class Config:
        use_enum_values = True
        # extra = Extra.forbid

    type: str = Field(description="The type of the value.")
    type_config: typing.Dict[str, typing.Any] = Field(
        description="Configuration for the type, in case it's complex.",
        default_factory=dict,
    )
    default: typing.Any = Field(
        description="A default value.", default=SpecialValue.NOT_SET
    )

    optional: bool = Field(
        description="Whether this value is required (True), or whether 'None' value is allowed (False).",
        default=False,
    )
    # required: typing.Any = Field(
    #     description="Whether this value is required to be set.", default=True
    # )

    doc: str = Field(
        default="-- n/a --",
        description="A description for the value of this input field.",
    )

    def is_required(self):

        if self.optional:
            return False
        else:
            if self.default in [None, SpecialValue.NOT_SET, SpecialValue.NO_VALUE]:
                return True
            else:
                return False

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


class ValueMetadata(BaseModel):

    origin: typing.Optional[str] = Field(
        description="Description of how/where the value was set.", default="n/a"
    )


class Value(BaseModel, JupyterMixin):
    """The underlying base class for all values.

    The important subclasses here are the ones inheriting from 'KiaraValue', as those are registered in the data
    registry.
    """

    class Config:
        extra = Extra.forbid
        use_enum_values = True

    def __init__(self, **data):  # type: ignore

        kiara = data.pop("kiara", None)
        if kiara is None:
            raise ValueError("No 'kiara' object provided.")
        self._kiara = kiara

        super().__init__(**data)

    _kiara: "Kiara" = PrivateAttr()
    _type_obj: ValueType = PrivateAttr(default=None)

    id: str = Field(description="A unique id for this value.")
    aliases: typing.List[str] = Field(
        description="Aliases for this value", default_factory=list
    )
    value_schema: ValueSchema = Field(description="The schema of this value.")
    is_constant: bool = Field(
        description="Whether this value is a constant.", default=False
    )
    last_update: typing.Optional[datetime] = Field(
        default=None, description="The time the last update to this value happened."
    )
    is_streaming: bool = Field(
        default=False,
        description="Whether the value is currently streamed into this object.",
    )
    is_set: bool = Field(
        description="Whether the value was set (in some way: user input, default, constant...).",
        default=False,
    )
    is_none: bool = Field(description="Whether the value is 'None'.", default=True)

    # value_hash: typing.Union[int, ValueHashMarker] = Field(
    #     description="The hash of the current value.", default=ValueHashMarker.NO_VALUE
    # )

    # is_valid: bool = Field(
    #     description="Whether the value is set and valid.", default=False
    # )
    value_metadata: ValueMetadata = Field(
        description="Base value metadata.", default_factory=ValueMetadata
    )
    metadata: typing.Dict[str, typing.Dict[str, typing.Any]] = Field(
        description="Metadata relating to the actual data (size, no. of rows, etc. -- depending on data type).",
        default_factory=dict,
    )

    @property
    def type_name(self) -> str:
        return self.value_schema.type

    @property
    def type_obj(self):
        if self._type_obj is None:
            cls = self._kiara.get_value_type_cls(self.value_schema.type)
            self._type_obj = cls(**self.value_schema.type_config)
        return self._type_obj

    def add_alias(self, alias):
        if alias not in self.aliases:
            self.aliases.append(alias)

    def item_is_valid(self) -> bool:

        if self.value_schema.optional:
            return True
        else:
            return not self.is_none

    def get_value_data(self) -> typing.Any:
        """Retrieve the actual data from the registry.

        This will be implemented by subclasses.
        """
        raise NotImplementedError()

    # def get_value_hash(self) -> str:
    #
    #     raise NotImplementedError()

    def _create_value_table(
        self, padding=(0, 1), ensure_metadata: bool = False
    ) -> Table:

        if ensure_metadata:
            self.get_metadata()

        table = Table(box=box.SIMPLE, show_header=False, padding=padding)
        table.add_column("property", style="i")
        table.add_column("value")

        table.add_row("id", self.id)  # type: ignore
        table.add_row("type", self.value_schema.type)
        table.add_row("desc", self.value_schema.doc)
        table.add_row("is set", "yes" if self.item_is_valid() else "no")
        table.add_row("is constant", "yes" if self.is_constant else "no")

        # if isinstance(self.value_hash, int):
        #     vh = str(self.value_hash)
        # else:
        #     vh = self.value_hash.value
        # table.add_row("hash", vh)
        if self.metadata:
            json_string = json.dumps(self.get_metadata(), indent=2)
            metadata = Syntax(json_string, "json")
            table.add_row("metadata", metadata)
        else:
            table.add_row("metadata", "-- no metadata --")

        return table

    def get_metadata(
        self, *metadata_keys: str, also_return_schema: bool = False
    ) -> typing.Mapping[str, typing.Mapping[str, typing.Any]]:

        if not metadata_keys:
            _metadata_keys = set(
                self._kiara.metadata_mgmt.get_metadata_keys_for_type(self.type_name)
            )
            for key in self.metadata.keys():
                _metadata_keys.add(key)
        else:
            _metadata_keys = set(metadata_keys)

        result = {}
        missing = set()
        for metadata_key in _metadata_keys:
            if metadata_key in self.metadata.keys():
                if also_return_schema:
                    result[metadata_key] = self.metadata[metadata_key]
                else:
                    result[metadata_key] = self.metadata[metadata_key]["metadata_item"]
            else:
                missing.add(metadata_key)

        if not missing:
            return result

        _md = self._kiara.metadata_mgmt.get_value_metadata(
            self, *missing, also_return_schema=True
        )
        for k, v in _md.items():
            self.metadata[k] = v
            if also_return_schema:
                result[k] = v
            else:
                result[k] = v["metadata_item"]
        return result

    def save(self) -> str:

        return self._kiara.data_store.save_value(self)

    # def transform(
    #     self,
    #     target_type: str,
    #     return_data: bool = True,
    #     config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    #     register_result: bool = False,
    # ) -> typing.Union[typing.Mapping[str, typing.Any], "Value"]:
    #
    #     transformed = self._kiara.transform_data(
    #         data=self,
    #         target_type=target_type,
    #         config=config,
    #         register_result=register_result,
    #     )
    #     if not return_data:
    #         return transformed
    #     else:
    #         return transformed.get_value_data()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        table = self._create_value_table()

        origin = self.value_metadata.origin
        if not origin:
            title = "Value"
        else:
            title = f"Value: [b]{origin}[/b]"

        yield Panel(table, box=box.ROUNDED, title_align="left", title=title)


class NonRegistryValue(Value):

    _value: typing.Any = PrivateAttr()

    def __init__(self, _init_value: typing.Any, **kwargs):  # type: ignore

        _id: typing.Optional[str] = kwargs.pop("id", None)
        if _id is None:
            _id = str(uuid.uuid4())

        self._value: typing.Any = _init_value

        if _init_value is None:
            is_set = False
            is_none = True
        else:
            is_set = True
            is_none = False

        kwargs["is_set"] = is_set
        kwargs["is_none"] = is_none

        super().__init__(id=_id, **kwargs)

    def get_value_data(self) -> typing.Any:

        if not self.is_set and self.value_schema.default not in (
            SpecialValue.NO_VALUE,
            SpecialValue.NOT_SET,
            None,
        ):
            return self.value_schema.default

        return self._value

    def set_value_data(self, value: typing.Any) -> bool:

        # TODO: validate against schema
        if value == self._value:
            return False
        self._value = value
        return True

    def __eq__(self, other):

        # TODO: compare all attributes if id is equal, just to make sure...

        if not isinstance(other, NonRegistryValue):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


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


class ValueSet(abc.ABC):
    @abc.abstractmethod
    def get_all_field_names(self) -> typing.Iterable[str]:
        pass

    @abc.abstractmethod
    def get_value_obj(
        self,
        field_name: str,
        ensure_metadata: typing.Union[bool, typing.Iterable[str], str] = False,
    ) -> Value:
        pass

    def get_all_value_objects(self) -> typing.Mapping[str, typing.Any]:
        return {fn: self.get_value_obj(fn) for fn in self.get_all_field_names()}

    @abc.abstractmethod
    def get_value_data_for_fields(
        self, *field_names: str
    ) -> typing.Dict[str, typing.Any]:
        pass

    def get_value_data(self, field_name: str):
        return self.get_value_data_for_fields(field_name)[field_name]

    def get_all_value_data(self) -> typing.Dict[str, typing.Any]:
        return self.get_value_data_for_fields(*self.get_all_field_names())

    def invalidate(self):

        inv = {}
        for field_name in self.get_all_field_names():
            inv[field_name] = SpecialValue.NO_VALUE
        self.set_values(**inv)

    def get_metadata(self) -> typing.Mapping[str, typing.Any]:
        return {}

    def is_read_only(self) -> bool:
        return True

    def set_values(self, **values: typing.Any) -> typing.Dict[Value, bool]:

        if self.is_read_only():
            raise Exception("Can't set values: this value set is read-only.")

        return self._set_values(**values)

    @abc.abstractmethod
    def _set_values(self, **values: typing.Any) -> typing.Dict[Value, bool]:
        """Set one or several values.

        Arguments:
            **values: the values to set (key: field_name, value: the data)

        Returns:
            a dictionary with each value item that was attempted to be set, and a boolean indicating whether it was updated (``True``), or the value remains unchanged (``False``)
        """

    def set_value(self, key: str, value: typing.Any) -> bool:

        r = self.set_values(**{key: value})
        assert len(r) == 1
        return next(iter(r.values()))

    def items_are_valid(self) -> bool:

        for field_name in self.get_all_field_names():
            item = self.get_value_obj(field_name)
            if not item.item_is_valid():
                return False
        return True

    def to_details(self, ensure_metadata: bool = False) -> "PipelineValues":

        result = {}
        for name in self.get_all_field_names():
            item = self.get_value_obj(name)
            result[name] = PipelineValue.from_value_obj(
                item, ensure_metadata=ensure_metadata
            )

        return PipelineValues(values=result)

    def _create_rich_table(
        self, show_headers: bool = True, ensure_metadata: bool = False
    ) -> Table:

        table = Table(box=box.SIMPLE, show_header=show_headers)
        table.add_column("Field name", style="i")
        table.add_column("Type")
        table.add_column("Description")
        table.add_column("Required")
        table.add_column("Is set")

        for k in self.get_all_field_names():
            v = self.get_value_obj(k, ensure_metadata=ensure_metadata)
            t = v.value_schema.type
            desc = v.value_schema.doc
            if not v.value_schema.is_required():
                req = "no"
            else:
                if (
                    v.value_schema.default
                    and v.value_schema.default != SpecialValue.NO_VALUE
                    and v.value_schema.default != SpecialValue.NOT_SET
                ):
                    req = "no"
                else:
                    req = "[red]yes[/red]"
            is_set = "yes" if v.item_is_valid() else "no"
            table.add_row(k, t, desc, req, is_set)

        return table

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        title = self.get_metadata().get("title", None)
        if title:
            postfix = f" [b]{title}[/b]"
        else:
            postfix = ""

        yield Panel(
            self._create_rich_table(show_headers=True),
            box=box.ROUNDED,
            title_align="left",
            title=f"Value-Set:{postfix}",
        )


class ValueSetImpl(ValueSet, typing.MutableMapping[str, Value]):
    """A dict-like object that contains a set of value fields that belong together in some way (for example outputs of a step or pipeline)."""

    @classmethod
    def from_schemas(
        cls,
        kiara: "Kiara",
        schemas: typing.Mapping[str, ValueSchema],
        read_only: bool = True,
        initial_values: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        title: typing.Optional[str] = None,
    ):

        values = {}
        for field_name, schema in schemas.items():
            _init_value = None

            if initial_values and initial_values.get(field_name, None) is not None:
                _init_value = initial_values[field_name]

            if not isinstance(_init_value, Value):
                value: Value = NonRegistryValue(value_schema=schema, _init_value=_init_value, kiara=kiara)  # type: ignore
            else:
                value = _init_value

            values[field_name] = value

        return cls(items=values, title=title, read_only=read_only)

    def __init__(
        self,
        items: typing.Mapping[str, Value],
        read_only: bool,
        title: typing.Optional[str] = None,
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
        self._read_only: bool = read_only
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

    def get_all_field_names(self) -> typing.Iterable[str]:
        return self._value_items.keys()

    def get_value_obj(
        self,
        field_name: str,
        ensure_metadata: typing.Union[bool, typing.Iterable[str], str] = False,
    ) -> Value:

        if field_name not in list(self.get_all_field_names()):
            raise KeyError(
                f"Field '{field_name}' not available in value set. Available fields: {', '.join(self.get_all_field_names())}"
            )
        obj: Value = self._value_items[field_name]

        if ensure_metadata:
            if isinstance(ensure_metadata, bool):
                obj.get_metadata()
            elif isinstance(ensure_metadata, str):
                obj.get_metadata(ensure_metadata)
            elif isinstance(ensure_metadata, typing.Iterable):
                obj.get_metadata(*ensure_metadata)
            else:
                raise ValueError(
                    f"Invalid type '{type(ensure_metadata)}' for 'ensure_metadata' argument."
                )

        return obj

    def get_value_data_for_fields(
        self, *field_names: str
    ) -> typing.Dict[str, typing.Any]:
        result = {}
        for k in field_names:
            v = self.get_value_obj(k)
            result[k] = v.get_value_data()

        return result

    def _set_values(self, **values: typing.Any) -> typing.Dict[Value, bool]:

        invalid: typing.List[str] = []
        registries: typing.Dict[DataRegistry, typing.Dict[Value, typing.Any]] = {}
        non_registry_values: typing.Dict[str, typing.Any] = {}

        for k, v in values.items():

            if k not in self._value_items.keys():
                invalid.append(k)
            else:
                item: Value = self._value_items[k]
                if not isinstance(item, NonRegistryValue):
                    registries.setdefault(item.registry, {})[item] = v  # type: ignore
                else:
                    non_registry_values[k] = v

        if invalid:
            raise ValueError(
                f"No value item(s) with name(s) {', '.join(invalid)} available, valid names: {', '.join(self._value_items.keys())}"
            )

        result: typing.Dict[Value, bool] = {}

        for registry, v in registries.items():
            _r = registry.set_values(v)
            result.update(_r)

        for k, v in non_registry_values.items():
            val_obj: NonRegistryValue = self[k]  # type: ignore
            result[val_obj] = val_obj.set_value_data(v)  # type: ignore

        return result

    def is_read_only(self):
        return self._read_only

    def get_metadata(self):
        return {"title": self._title}

    def to_dict(self) -> typing.Dict[str, typing.Any]:

        return self.to_details().dict()

    def to_json(self) -> str:

        return self.to_details().json()

    def __repr__(self):

        return (
            f"ValueSetImpl(values={self._value_items} valid={self.items_are_valid()})"
        )


ValueSchema.update_forward_refs()


class ValuesInfo(object):
    def __init__(self, value_set: ValueSet, title: typing.Optional[str] = None):

        self._value_set: ValueSet = value_set
        self._title: typing.Optional[str] = title

    # def create_value_data_table(
    #     self,
    #     show_headers: bool = False,
    #     convert_module_type: typing.Optional[str] = None,
    #     convert_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    # ) -> Table:
    #
    #     table = Table(show_header=show_headers, box=box.SIMPLE)
    #     table.add_column("Field name", style="i")
    #     table.add_column("Value data")
    #
    #     for field_name in self._value_set.get_all_field_names():
    #         value = self._value_set.get_value_obj(field_name)
    #
    #         if not value.is_set:
    #             if value.item_is_valid():
    #                 value_str: typing.Union[
    #                     ConsoleRenderable, RichCast, str
    #                 ] = "-- not set --"
    #             else:
    #                 value_str = "[red]-- not set --[/red]"
    #         elif value.is_none:
    #             if value.item_is_valid():
    #                 value_str = "-- no value --"
    #             else:
    #                 value_str = "[red]-- no value --[/red]"
    #         else:
    #             if not convert_module_type:
    #                 value_str = value.get_value_data()
    #             else:
    #                 _value_str = value.transform(
    #                     convert_module_type, return_data=True, config=convert_config
    #                 )
    #
    #             if not isinstance(_value_str, (ConsoleRenderable, RichCast, str)):
    #                 value_str = str(value_str)
    #
    #         table.add_row(field_name, value_str)
    #
    #     return table

    def create_value_info_table(
        self, show_headers: bool = False, ensure_metadata: bool = False
    ) -> Table:

        table = Table(show_header=show_headers, box=box.SIMPLE)
        table.add_column("Field name", style="i")
        table.add_column("Value info")

        for field_name in self._value_set.get_all_field_names():
            details = self._value_set.get_value_obj(field_name)
            if not details.is_set:
                if details.item_is_valid():
                    value_info: typing.Union[
                        str, Table
                    ] = "[green]-- not set --[/green]"
                else:
                    value_info = "[red]-- not set --[/red]"
            elif details.is_none:
                if details.item_is_valid():
                    value_info = "-- no value --"
                else:
                    value_info = "[red]-- no value --[/red]"
            else:
                if ensure_metadata:
                    details.get_metadata()

                value_info = details._create_value_table()
            table.add_row(field_name, value_info)

        return table


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
            aliases=value.aliases,
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
    aliases: typing.List[str] = Field(description="Aliases for this value.")
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


class ValueSetType(ValueType):
    def validate(cls, value: typing.Any) -> None:
        assert isinstance(value, ValueSet)

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional[ValueType]:

        if isinstance(data, ValueSet):
            return ValueSetType()
        else:
            return None

    @classmethod
    def python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [ValueSet]


Value.update_forward_refs()
DataValue.update_forward_refs()
LinkedValue.update_forward_refs()
StepInputField.update_forward_refs()
StepOutputField.update_forward_refs()
