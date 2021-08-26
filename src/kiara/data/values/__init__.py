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
from kiara.defaults import SpecialValue
from kiara.module_config import ModuleConfig
from kiara.utils import StringYAML

if typing.TYPE_CHECKING:
    from kiara.data.registry import DataRegistry, ValueSlotUpdateHandler
    from kiara.kiara import Kiara

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
    is_constant: bool = Field(
        description="Whether the value is a constant.", default=False
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


class ValueSeed(BaseModel, abc.ABC):
    pass


class ModuleValueSeed(ModuleConfig):

    result_name: str = Field(
        description="The result field name for the value this refers to."
    )
    inputs: typing.Dict[str, "Value"] = Field(
        description="The inputs that were used to create the value this refers to."
    )


class Value(BaseModel, JupyterMixin):
    """The underlying base class for all values.

    The important subclasses here are the ones inheriting from 'PipelineValue', as those are registered in the data
    registry.
    """

    class Config:
        extra = Extra.forbid
        use_enum_values = True

    def __init__(self, value_data: typing.Any = SpecialValue.NOT_SET, value_schema: typing.Optional[ValueSchema] = None, is_constant: typing.Optional[bool] = False, value_seed: typing.Optional[ValueSeed] = None, kiara: typing.Optional["Kiara"] = None, registry: typing.Optional["DataRegistry"] = None):  # type: ignore

        if kiara is None:
            from kiara.kiara import Kiara

            kiara = Kiara.instance()

        if value_schema is None:
            raise NotImplementedError()

        if value_schema.is_constant and value_data not in [
            SpecialValue.NO_VALUE,
            SpecialValue.NOT_SET,
            None,
        ]:
            raise Exception(
                "Can't create value. Is a constant, but value data was provided."
            )

        cls = kiara.get_value_type_cls(value_schema.type)
        _type_obj = cls(**value_schema.type_config)

        if value_data not in [SpecialValue.NO_VALUE, SpecialValue.NOT_SET, None]:
            # TODO: should we keep the original value?
            value_data = _type_obj.import_value(value_data)

        self._kiara = kiara
        if registry is None:
            registry = self._kiara.data_registry
        self._registry = registry

        kwargs: typing.Dict[str, typing.Any] = {}
        kwargs["id"] = str(uuid.uuid4())

        kwargs["value_schema"] = value_schema
        if value_schema.is_constant:
            value_data = value_schema.default
            is_constant = True

        kwargs["is_constant"] = is_constant

        if value_seed is None:
            value_seed = ValueSeed()

        kwargs["value_seed"] = value_seed

        kwargs["is_streaming"] = False  # not used yet
        kwargs["metadata"] = {}
        kwargs["creation_date"] = datetime.now()

        super().__init__(**kwargs)

        self._type_obj = _type_obj

        self._registry._register_value(self, data=value_data)

        self.is_set = value_data != SpecialValue.NOT_SET
        self.is_none = value_data in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]

    _kiara: "Kiara" = PrivateAttr()
    _registry: "DataRegistry" = PrivateAttr()
    _type_obj: ValueType = PrivateAttr(default=None)
    _hash_cache: typing.Optional[str] = PrivateAttr(default=None)

    id: str = Field(description="A unique id for this value.")
    value_schema: ValueSchema = Field(description="The schema of this value.")
    value_seed: typing.Optional[ValueSeed] = Field(
        description="Information about how this value was created."
    )
    is_constant: bool = Field(
        description="Hint whether this value is a constant.", default=False
    )
    creation_date: typing.Optional[datetime] = Field(
        description="The time this value was created value happened."
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

    metadata: typing.Dict[str, typing.Dict[str, typing.Any]] = Field(
        description="Currently available relating to the actual data (size, no. of rows, etc. -- depending on data type). This attribute is not populated by default, use the 'get_metadata()' method to request one or several metadata items."
    )

    @property
    def type_name(self) -> str:
        return self.value_schema.type

    @property
    def type_obj(self):
        # if self._type_obj is None:
        #     cls = self._kiara.get_value_type_cls(self.value_schema.type)
        #     self._type_obj = cls(**self.value_schema.type_config)
        return self._type_obj

    def calculate_hash(self, hash_type: str) -> str:
        """Calculate the hash of a specified type for this value. Hashes are cached."""
        if self._hash_cache is None:
            self._hash_cache = self.type_obj.calculate_value_hash(
                self.get_value_data(), hash_type=hash_type
            )
        return self._hash_cache

    def item_is_valid(self) -> bool:
        """Check whether the current value is valid"""

        if self.value_schema.optional:
            return True
        else:
            return self.is_set and not self.is_none

    def get_value_data(self) -> typing.Any:

        return self._registry.get_value_data(self)

    # def update_value(self, data: typing.Any) -> "Value":
    #
    #     return self._registry.update_value_slot(value_slot=self, data=data)

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
        """Retrieve (if necessary) and return metadata for the specified keys.

        By default, the metadata is returned as a map (in case of a single key: single-item map) with metadata key as
        dict key, and the metadata as value. If 'also_return_schema' was set to `True`, the value will be a two-key
        map with the metadata under the ``metadata_item`` subkey, and the value schema under ``metadata_schema``.

        If no metadata key is specified, all available metadata for this value type will be returned.

        Arguments:
            metadata_keys: the metadata keys to retrieve metadata (empty for all available metadata)
            also_return_schema: whether to also return the schema for each metadata item
        """

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
        """Save this value, under the specified alias(es)."""

        return self._kiara.data_store.save_value(self, aliases=aliases)

    def __eq__(self, other):

        # TODO: compare all attributes if id is equal, just to make sure...

        if not isinstance(other, Value):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):

        return f"Value(id={self.id}, type={self.type_name}, is_set={self.is_set}, is_none={self.is_none}, is_constant={self.is_constant}"

    def __str__(self):

        return self.__repr__()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        table = self._create_value_table()

        title = "Value"
        yield Panel(table, box=box.ROUNDED, title_align="left", title=title)


ValueSchema.update_forward_refs()
Value.update_forward_refs()


class ValueSlot(BaseModel):
    @classmethod
    def from_value(cls, id: str, value: Value) -> "ValueSlot":

        vs = ValueSlot.from_value_schema(
            id=id, value_schema=value.value_schema, kiara=value._kiara
        )
        vs.add_value(value)
        return vs

    @classmethod
    def from_value_schema(
        cls, id: str, value_schema: ValueSchema, kiara: "Kiara"
    ) -> "ValueSlot":

        vs = ValueSlot(id=id, value_schema=value_schema, kiara=kiara)
        return vs

    def __init__(self, **data):  # type: ignore
        _kiara = data.pop("kiara", None)
        if _kiara is None:
            raise Exception("No kiara context provided.")
        _registry = data.pop("registry", None)
        if _registry is None:
            _registry = _kiara.data_registry

        self._kiara = _kiara
        self._registry = _registry
        super().__init__(**data)

    _kiara: "Kiara" = PrivateAttr()
    _registry: "DataRegistry" = PrivateAttr()
    _callbacks: typing.Dict[str, "ValueSlotUpdateHandler"] = PrivateAttr(
        default_factory=dict
    )

    id: str = Field(description="The id for this slot.")
    value_schema: ValueSchema = Field(
        description="The schema for the values of this slot."
    )
    values: typing.Dict[int, Value] = Field(
        description="The values of this slot, with versions as key.",
        default_factory=dict,
    )

    @property
    def latest_version_nr(self) -> int:
        if not self.values:
            return 0
        return max(self.values.keys())

    def get_latest_value(self) -> Value:

        lv = self.latest_version_nr
        if lv == 0:
            raise Exception("No value added to value slot yet.")

        return self.values[self.latest_version_nr]

    def register_callbacks(self, *callbacks: "ValueSlotUpdateHandler"):

        for cb in callbacks:
            cb_id: typing.Optional[str] = None
            if cb_id in self._callbacks.keys():
                raise Exception(f"Callback with id '{cb_id}' already registered.")
            if hasattr(cb, "id"):
                if callable(cb.id):  # type: ignore
                    cb_id = cb.id()  # type: ignore
                else:
                    cb_id = cb.id  # type: ignore
            elif hasattr(cb, "get_id"):
                cb_id = cb.get_id()  # type: ignore

            if cb_id is None:
                cb_id = str(uuid.uuid4())

            assert isinstance(cb_id, str)

            self._callbacks[cb_id] = cb

    def add_value(self, value: Value, trigger_callbacks: bool = True) -> int:
        """Add a value to this slot."""

        if self.latest_version_nr != 0 and value.id == self.get_latest_value().id:
            return self.latest_version_nr

        # TODO: check value data equality

        version = self.latest_version_nr + 1
        assert version not in self.values.keys()
        self.values[version] = value

        if trigger_callbacks:
            for cb in self._callbacks.values():
                cb.values_updated(self)

        return version

    def is_latest_value(self, value: Value):

        return value.id == self.get_latest_value().id

    def __eq__(self, other):

        if not isinstance(other, ValueSlot):
            return False

        return self.id == other.id

    def __hash__(self):

        return hash(self.id)
