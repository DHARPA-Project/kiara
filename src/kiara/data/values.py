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
from pydantic import BaseModel, Extra, Field, PrivateAttr
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.jupyter import JupyterMixin
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from kiara.data.store import SavedValueMetadata
from kiara.data.types import ValueType
from kiara.defaults import INVALID_VALUE_NAMES, SpecialValue
from kiara.utils import StringYAML

if typing.TYPE_CHECKING:
    from kiara.data.registry import DataRegistry
    from kiara.kiara import Kiara
    from kiara.pipeline import PipelineValues

log = logging.getLogger("kiara")
yaml = StringYAML()


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
    _hash_cache: typing.Optional[str] = PrivateAttr(default=None)

    id: str = Field(description="A unique id for this value.")
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

    def calculate_hash(self, hash_type: str) -> str:
        if self._hash_cache is None:
            self._hash_cache = self.type_obj.calculate_value_hash(
                self.get_value_data(), hash_type=hash_type
            )
        return self._hash_cache

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

    def save(
        self, aliases: typing.Optional[typing.Iterable[str]] = None
    ) -> SavedValueMetadata:

        return self._kiara.data_store.save_value(self, aliases=aliases)

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
        self._hash_cache = None
        self._value = value
        return True

    def __eq__(self, other):

        # TODO: compare all attributes if id is equal, just to make sure...

        if not isinstance(other, NonRegistryValue):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class ValueSet(typing.MutableMapping[str, Value]):
    """A dict-like collection of values, with their field_names as keys, and a Value object as value."""

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

        from kiara.pipeline import PipelineValue, PipelineValues

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


class ValueSetImpl(ValueSet):
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
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [ValueSet]


Value.update_forward_refs()
