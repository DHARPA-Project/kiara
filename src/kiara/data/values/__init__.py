# -*- coding: utf-8 -*-

"""A module that contains value-related classes for *Kiara*.

A value in Kiara-speak is a pointer to actual data (aka 'bytes'). It contains metadata about that data (like whether it's
valid/set, what type/schema it has, when it was last modified, ...), but it does not contain the data itself. The reason for
that is that such data can be fairly large, and in a lot of cases it is not necessary for the code involved to have
access to it, access to the metadata is enough.

Each Value has a unique id, which can be used to retrieve the data (whole, or parts of it) from a [DataRegistry][kiara.data.registry.DataRegistry].
In addition, that id can be used to subscribe to change events for a value (published whenever the data that is associated with a value was changed).
"""

import json
import logging
import typing
import uuid
from datetime import datetime
from pydantic import BaseModel, Extra, Field, PrivateAttr
from rich import box
from rich.console import Console, ConsoleOptions, RenderableType, RenderResult
from rich.jupyter import JupyterMixin
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.tree import Tree

from kiara.data.types import ValueType
from kiara.defaults import COLOR_LIST, SpecialValue
from kiara.info import KiaraInfoModel
from kiara.metadata.core_models import DocumentationMetadataModel
from kiara.metadata.data import DeserializeConfig
from kiara.module_config import ModuleConfig
from kiara.utils import StringYAML, is_debug, log_message

if typing.TYPE_CHECKING:
    from kiara.data.registry import (
        BaseDataRegistry,
        DataRegistry,
        ValueSlotUpdateHandler,
    )
    from kiara.kiara import Kiara
    from kiara.module import KiaraModule

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


class ValueLineage(ModuleConfig):
    @classmethod
    def from_module_and_inputs(
        cls,
        module: "KiaraModule",
        output_name: str,
        inputs: typing.Mapping[str, typing.Union["Value", "ValueInfo"]],
    ):

        module_type = module._module_type_id  # type: ignore
        module_config = module.config.dict()
        doc = module.get_type_metadata().documentation

        _inputs = {}
        for field_name, value in inputs.items():
            if isinstance(value, Value):
                _inputs[field_name] = value.get_info()
            else:
                _inputs[field_name] = value

        return ValueLineage.construct(
            module_type=module_type,
            module_config=module_config,
            doc=doc,
            output_name=output_name,
            inputs=_inputs,
        )

    @classmethod
    def create(
        cls,
        module_type: str,
        module_config: typing.Mapping[str, typing.Any],
        module_doc: DocumentationMetadataModel,
        output_name: str,
        inputs: typing.Mapping[str, typing.Union["Value", "ValueInfo"]],
    ):

        _inputs = {}
        for field_name, value in inputs.items():
            if isinstance(value, Value):
                _inputs[field_name] = value.get_info()
            else:
                _inputs[field_name] = value

        return ValueLineage.construct(
            module_type=module_type,
            module_config=dict(module_config),
            doc=module_doc,
            output_name=output_name,
            inputs=_inputs,
        )

    output_name: str = Field(
        description="The result field name for the value this refers to."
    )
    inputs: typing.Dict[str, "ValueInfo"] = Field(
        description="The inputs that were used to create the value this refers to."
    )
    value_index: typing.Optional[typing.Dict[str, "ValueInfo"]] = Field(
        description="Index of all values that are associated with this value lineage.",
        default=None,
    )

    def to_minimal_dict(
        self,
        include_metadata: bool = False,
        include_module_doc: bool = False,
        include_module_config: bool = True,
    ) -> typing.Dict[str, typing.Any]:

        full_dict = self.dict(exclude_none=True)
        minimal_dict = filter_metadata_schema(
            full_dict,
            include_metadata=include_metadata,
            include_module_doc=include_module_doc,
            include_module_config=include_module_config,
        )
        return minimal_dict

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        all_ids = sorted(find_all_ids_in_lineage(self))
        id_color_map = {}
        for idx, v_id in enumerate(all_ids):
            id_color_map[v_id] = COLOR_LIST[idx % len(COLOR_LIST)]

        show_ids = config.get("include_ids", False)

        tree = fill_lineage_tree(self, include_ids=show_ids)
        return tree


def filter_metadata_schema(
    data: typing.Mapping[str, typing.Any],
    include_metadata: bool = False,
    include_module_doc: bool = False,
    include_module_config: bool = True,
) -> typing.Dict[str, typing.Any]:

    result = {}
    for k, v in data.items():

        if (
            isinstance(v, typing.Mapping)
            and "metadata_item" in v.keys()
            and not include_metadata
        ):
            result[k] = v["metadata_item"]
        elif isinstance(v, typing.Mapping):
            if k == "doc" and not include_module_doc:
                continue
            elif k == "module_config" and not include_module_config:
                continue
            else:
                result[k] = filter_metadata_schema(
                    v,
                    include_metadata=include_metadata,
                    include_module_doc=include_module_doc,
                )
        else:
            result[k] = v

    return result


def find_all_ids_in_lineage(lineage: ValueLineage, ids: typing.Set[str] = None):

    if ids is None:
        ids = set()

    if not lineage:
        return ids

    for input_name, value_info in lineage.inputs.items():
        ids.add(value_info.value_id)
        if value_info.lineage:
            find_all_ids_in_lineage(value_info.lineage, ids=ids)

    return ids


def fill_lineage_tree(
    lineage: ValueLineage,
    node: typing.Optional[Tree] = None,
    include_ids: bool = False,
    level=0,
):

    color = COLOR_LIST[level % len(COLOR_LIST)]
    if node is None:
        main = Tree(f"[b {color}]{lineage.module_type}[/b {color}]")
    else:
        main = node.add(f"[b {color}]{lineage.module_type}[/b {color}]")

    for input_name in sorted(lineage.inputs.keys()):

        value_info = lineage.inputs[input_name]

        value_type = value_info.value_schema.type
        if include_ids:
            v_id_str = f" = {value_info.value_id}"
        else:
            v_id_str = ""
        input_node = main.add(
            f"input: [i {color}]{input_name} ({value_type})[/i {color}]{v_id_str}"
        )
        if value_info.lineage:
            fill_lineage_tree(
                value_info.lineage, input_node, level=level + 1, include_ids=include_ids
            )

    return main


class ValueHash(BaseModel):

    hash: str = Field(description="The value hash.")
    hash_type: str = Field(description="The value hash method.")


NO_ID_YET_MARKER = "__no_id_yet__"


class Value(BaseModel, JupyterMixin):
    """The underlying base class for all values.

    The important subclasses here are the ones inheriting from 'PipelineValue', as those are registered in the data
    registry.
    """

    class Config:
        extra = Extra.forbid
        use_enum_values = True

    def __init__(self, registry: "BaseDataRegistry", value_schema: ValueSchema, type_obj: ValueType, is_set: bool, is_none: bool, hashes: typing.Optional[typing.Iterable[ValueHash]] = None, metadata: typing.Optional[typing.Mapping[str, typing.Dict[str, typing.Any]]] = None, register_token: typing.Optional[uuid.UUID] = None):  # type: ignore

        if not register_token:
            raise Exception("No register token provided.")

        if not registry._check_register_token(register_token):
            raise Exception(
                f"Value registration with token '{register_token}' not allowed."
            )

        if value_schema is None:
            raise NotImplementedError()

        assert registry
        self._registry = registry
        self._kiara = self._registry._kiara

        kwargs: typing.Dict[str, typing.Any] = {}
        kwargs["id"] = NO_ID_YET_MARKER

        kwargs["value_schema"] = value_schema

        # if value_lineage is None:
        #     value_lineage = ValueLineage()
        #
        # kwargs["value_lineage"] = value_lineage

        # kwargs["is_streaming"] = False  # not used yet
        kwargs["creation_date"] = datetime.now()
        kwargs["is_set"] = is_set
        kwargs["is_none"] = is_none

        if hashes:
            kwargs["hashes"] = list(hashes)

        if metadata:
            kwargs["metadata"] = dict(metadata)
        else:
            kwargs["metadata"] = {}

        super().__init__(**kwargs)
        self._type_obj = type_obj

    _kiara: "Kiara" = PrivateAttr()
    _registry: "BaseDataRegistry" = PrivateAttr()
    _type_obj: ValueType = PrivateAttr()
    _value_info: "ValueInfo" = PrivateAttr(default=None)

    id: str = Field(description="A unique id for this value.")
    value_schema: ValueSchema = Field(description="The schema of this value.")
    creation_date: typing.Optional[datetime] = Field(
        description="The time this value was created value happened."
    )
    # is_streaming: bool = Field(
    #     default=False,
    #     description="Whether the value is currently streamed into this object.",
    # )
    is_set: bool = Field(
        description="Whether the value was set (in some way: user input, default, constant...).",
        default=False,
    )
    is_none: bool = Field(description="Whether the value is 'None'.", default=True)
    hashes: typing.List[ValueHash] = Field(
        description="Available hashes relating to the actual value data. This attribute is not populated by default, use the 'get_hashes()' method to request one or several hash items, afterwards those hashes will be reflected in this attribute.",
        default_factory=list,
    )

    metadata: typing.Dict[str, typing.Dict[str, typing.Any]] = Field(
        description="Available metadata relating to the actual value data (size, no. of rows, etc. -- depending on data type). This attribute is not populated by default, use the 'get_metadata()' method to request one or several metadata items, afterwards those metadata items will be reflected in this attribute.",
        default_factory=dict,
    )

    @property
    def type_name(self) -> str:
        return self.value_schema.type

    @property
    def type_obj(self):
        """Return the object that contains all the type information for this value."""
        return self._type_obj

    def get_hash(self, hash_type: str) -> ValueHash:
        """Calculate the hash of a specified type for this value. Hashes are cached."""

        hashes = self.get_hashes(hash_type)
        return list(hashes)[0]

    def get_hashes(self, *hash_types: str) -> typing.Iterable[ValueHash]:

        all_hash_types = self.type_obj.get_supported_hash_types()
        if not hash_types:
            try:
                hash_types = all_hash_types
            except Exception as e:
                log_message(str(e))

            if not hash_types:
                return []

        result = []
        missing = list(hash_types)
        for hash_obj in self.hashes:
            if hash_obj.hash_type in hash_types:
                result.append(hash_obj)
                missing.remove(hash_obj.hash_type)

        for hash_type in missing:
            if hash_type not in all_hash_types:
                raise Exception(
                    f"Hash type '{hash_type}' not supported for '{self.type_name}'"
                )

            hash_str = self.type_obj.calculate_value_hash(
                value=self.get_value_data(), hash_type=hash_type
            )
            hash_obj = ValueHash(hash_type=hash_type, hash=hash_str)
            self.hashes.append(hash_obj)
            result.append(hash_obj)

        return result

    def item_is_valid(self) -> bool:
        """Check whether the current value is valid"""

        if self.value_schema.optional:
            return True
        else:
            return self.is_set and not self.is_none

    def get_value_data(self) -> typing.Any:

        return self._registry.get_value_data(self)

    def get_lineage(self) -> typing.Optional[ValueLineage]:

        return self._registry.get_lineage(self)

    def set_value_lineage(self, value_lineage: ValueLineage) -> None:

        if hasattr(self._registry, "set_value_lineage"):
            return self._registry.set_value_lineage(self, value_lineage)  # type: ignore
        else:
            raise Exception("Can't set value lineage: registry is read only")

    def get_info(self) -> "ValueInfo":

        if self._value_info is None:
            self._value_info = ValueInfo.from_value(self)
        return self._value_info

    def create_info(self, include_deserialization_config: bool = False) -> "ValueInfo":

        return ValueInfo.from_value(
            self, include_deserialization_config=include_deserialization_config
        )

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
        self,
        aliases: typing.Optional[typing.Iterable[str]] = None,
        register_missing_aliases: bool = True,
    ) -> "Value":
        """Save this value, under the specified alias(es)."""

        # if self.get_value_lineage():
        #     for field_name, value in self.get_value_lineage().inputs.items():
        #         value_obj = self._registry.get_value_obj(value)
        #         try:
        #             value_obj.save()
        #         except Exception as e:
        #             print(e)

        value = self._kiara.data_store.register_data(self)
        if aliases:
            self._kiara.data_store.link_aliases(
                value, *aliases, register_missing_aliases=register_missing_aliases
            )

        return value

    def __eq__(self, other):

        # TODO: compare all attributes if id is equal, just to make sure...

        if not isinstance(other, Value):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):

        return f"Value(id={self.id}, type={self.type_name}, is_set={self.is_set}, is_none={self.is_none}"

    def __str__(self):

        return self.__repr__()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        # table = self._create_value_table()

        title = "Value"
        yield Panel(self.get_info(), box=box.ROUNDED, title_align="left", title=title)


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
    tags: typing.Dict[str, int] = Field(
        description="The tags for this value slot (tag name as key, linked version as value.",
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

    def add_value(
        self,
        value: Value,
        trigger_callbacks: bool = True,
        tags: typing.Optional[typing.Iterable[str]] = None,
    ) -> int:
        """Add a value to this slot."""

        if self.latest_version_nr != 0 and value.id == self.get_latest_value().id:
            return self.latest_version_nr

        # TODO: check value data equality
        if self.value_schema.is_constant and self.values:
            if is_debug():
                import traceback

                traceback.print_stack()

            raise Exception("Can't add value: value slot marked as 'constant'.")

        version = self.latest_version_nr + 1
        assert version not in self.values.keys()
        self.values[version] = value
        if tags:
            for tag in tags:
                self.tags[tag] = version

        if trigger_callbacks:
            for cb in self._callbacks.values():
                cb.values_updated(self)

        return version

    def is_latest_value(self, value: Value):

        return value.id == self.get_latest_value().id

    def find_linked_aliases(
        self, value_item: typing.Union[Value, str]
    ) -> typing.List["ValueAlias"]:

        if isinstance(value_item, Value):
            value_item = value_item.id

        result = []
        for _version, _value in self.values.items():
            if _value.id == value_item:
                va = ValueAlias(alias=self.id, version=_version)
                result.append(va)
                if _version in self.tags.values():
                    for _tag, _tag_version in self.tags.items():
                        if _tag_version == _version:
                            va = ValueAlias(alias=self.id, tag=_tag)
                            result.append(va)

        return result

    def __eq__(self, other):

        if not isinstance(other, ValueSlot):
            return False

        return self.id == other.id

    def __hash__(self):

        return hash(self.id)


class ValueAlias(BaseModel):
    @classmethod
    def from_string(
        self, value_alias: str, default_repo_name: typing.Optional[str] = None
    ) -> "ValueAlias":

        if not isinstance(value_alias, str):
            raise Exception("Invalid id_or_alias: not a string.")
        if not value_alias:
            raise Exception("Invalid id_or_alias: can't be empty string.")

        _repo_name: typing.Optional[str] = default_repo_name
        _version: typing.Optional[int] = None
        _tag: typing.Optional[str] = None

        if "#" in value_alias:
            _repo_name, _value_alias = value_alias.split("#", maxsplit=1)
        else:
            _value_alias = value_alias

        if "@" in _value_alias:
            _alias, _postfix = _value_alias.split("@", maxsplit=1)

            try:
                _version = int(_postfix)
            except ValueError:
                if not _postfix.isidentifier():
                    raise Exception(
                        f"Invalid format for version/tag element of id_or_alias: {_tag}"
                    )
                _tag = _postfix
        else:
            _alias = _value_alias

        return ValueAlias(
            repo_name=_repo_name, alias=_alias, version=_version, tag=_tag
        )

    @classmethod
    def from_strings(
        cls, *value_aliases: typing.Union[str, "ValueAlias"]
    ) -> typing.List["ValueAlias"]:

        result = []
        for va in value_aliases:
            if isinstance(va, str):
                result.append(ValueAlias.from_string(va))
            elif isinstance(va, ValueAlias):
                result.append(va)
            else:
                raise TypeError(
                    f"Invalid type '{type(va)}' for type alias, expected 'str' or 'ValueAlias'."
                )
        return result

    repo_name: typing.Optional[str] = Field(
        description="The name of the data repo the value lives in.", default=None
    )
    alias: str = Field("The alias name.")
    version: typing.Optional[int] = Field(
        description="The version of this alias.", default=None
    )
    tag: typing.Optional[str] = Field(
        description="The tag for the alias.", default=None
    )

    @property
    def full_alias(self):
        if self.tag is not None:
            return f"{self.alias}@{self.tag}"
        elif self.version is not None:
            return f"{self.alias}@{self.version}"
        else:
            return self.alias


class ValueInfo(KiaraInfoModel):
    @classmethod
    def from_value(cls, value: Value, include_deserialization_config: bool = False):

        if value.id not in value._registry.value_ids:
            raise Exception("Value not registered (yet).")

        # aliases = value._registry.find_aliases_for_value(value)
        hashes = value.get_hashes()
        metadata = value.get_metadata(also_return_schema=True)
        # metadata = value.metadata
        value_lineage = value.get_lineage()

        if include_deserialization_config:
            # serialize_operation: SerializeValueOperationType = (  # type: ignore
            #     value._kiara.operation_mgmt.get_operation("serialize")  # type: ignore
            # )
            raise NotImplementedError()
        return ValueInfo(
            value_id=value.id,
            value_schema=value.value_schema,
            hashes=hashes,
            metadata=metadata,
            lineage=value_lineage,
            is_valid=value.item_is_valid(),
        )

    value_id: str = Field(description="The value id.")
    value_schema: ValueSchema = Field(description="The value schema.")
    # aliases: typing.List[ValueAlias] = Field(
    #     description="All aliases for this value.", default_factory=list
    # )
    # tags: typing.List[str] = Field(
    #     description="All tags for this value.", default_factory=list
    # )
    # created: str = Field(description="The time the data was created.")
    is_valid: bool = Field(
        description="Whether the item is valid (in the context of its schema)."
    )
    hashes: typing.List[ValueHash] = Field(
        description="All available hashes for this value.", default_factory=list
    )
    metadata: typing.Dict[str, typing.Dict[str, typing.Any]] = Field(
        description="The metadata associated with this value."
    )
    lineage: typing.Optional[ValueLineage] = Field(
        description="Information about how the value was created.", default=None
    )
    deserialize_config: typing.Optional[DeserializeConfig] = Field(
        description="The module config (incl. inputs) to deserialize the value.",
        default=None,
    )

    def get_metadata_items(self, *keys: str) -> typing.Dict[str, typing.Any]:

        if not keys:
            _keys: typing.Iterable[str] = self.metadata.keys()
        else:
            _keys = keys

        result = {}
        for k in _keys:

            md = self.metadata.get(k)
            if md is None:
                raise Exception(f"No metadata for key '{k}' available.")

            result[k] = md["metadata_item"]

        return result

    def get_metadata_schemas(self, *keys: str) -> typing.Dict[str, typing.Any]:

        if not keys:
            _keys: typing.Iterable[str] = self.metadata.keys()
        else:
            _keys = keys

        result = {}
        for k in _keys:
            md = self.metadata.get(k)
            if md is None:
                raise Exception(f"No metadata for key '{k}' available.")
            result[k] = md["metadata_schema"]

        return result

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        padding = config.get("padding", (0, 1))
        skip_metadata = config.get("skip_metadata", False)
        skip_value_lineage = config.get("skip_lineage", True)
        include_ids = config.get("include_ids", False)

        table = Table(box=box.SIMPLE, show_header=False, padding=padding)
        table.add_column("property", style="i")
        table.add_column("value")

        table.add_row("id", self.value_id)  # type: ignore
        table.add_row("type", self.value_schema.type)
        if self.value_schema.type_config:
            json_data = json.dumps(self.value_schema.type_config)
            tc_content = Syntax(json_data, "json")
            table.add_row("type config", tc_content)
        table.add_row("desc", self.value_schema.doc)
        table.add_row("is set", "yes" if self.is_valid else "no")
        # table.add_row("is constant", "yes" if self.is_constant else "no")

        # if isinstance(self.value_hash, int):
        #     vh = str(self.value_hash)
        # else:
        #     vh = self.value_hash.value
        # table.add_row("hash", vh)

        if self.hashes:
            hashes_dict = {hs.hash_type: hs.hash for hs in self.hashes}
            yaml_string = yaml.dump(hashes_dict)
            hases_str = Syntax(yaml_string, "yaml", background_color="default")
            table.add_row("", "")
            table.add_row("hashes", hases_str)

        if not skip_metadata:
            if self.metadata:
                yaml_string = yaml.dump(data=self.get_metadata_items())
                # json_string = json.dumps(self.get_metadata_items(), indent=2)
                metadata = Syntax(yaml_string, "yaml", background_color="default")
                table.add_row("metadata", metadata)
            else:
                table.add_row("metadata", "-- no metadata --")

        if not skip_value_lineage and self.lineage:
            if self.metadata:
                table.add_row("", "")
            # json_string = self.lineage.json(indent=2)
            # seed_content = Syntax(json_string, "json")
            table.add_row(
                "lineage", self.lineage.create_renderable(include_ids=include_ids)
            )

        return table


ValueLineage.update_forward_refs()
