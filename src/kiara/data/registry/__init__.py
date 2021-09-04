# -*- coding: utf-8 -*-
import abc
import re
import typing
import uuid

from kiara.data import Value
from kiara.data.types import ValueType
from kiara.data.values import (
    NO_ID_YET_MARKER,
    ValueAlias,
    ValueHash,
    ValueSchema,
    ValueSeed,
    ValueSlot,
)
from kiara.defaults import SpecialValue

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara

try:

    class ValueSlotUpdateHandler(typing.Protocol):
        """The call signature for callbacks that can be registered as value update handlers."""

        def values_updated(self, *items: "ValueSlot") -> typing.Any:
            ...


except Exception:
    # there is some issue with older Python only_latest, typing.Protocol, and Pydantic
    ValueUpdateHandler = typing.Callable  # type:ignore


class BaseDataRegistry(abc.ABC):
    """Base class to extend if you want to write a *kiara* data registry.

    This outlines the main methods that must be available for an entity that holds some values and their data, as
    well as their associated aliases. In most cases users will interact with the 'official' [DataRegistry][kiara.data.registry.DataRegistry]
    class, but there are cases where it will make sense to have different implementations (like for example read-only
    'archive' data registries).
    """

    def __init__(self, kiara: "Kiara"):

        self._id: str = str(uuid.uuid4())
        self._kiara: Kiara = kiara
        self._hashes: typing.Dict[str, typing.Dict[str, str]] = {}
        self._seeds: typing.Dict[str, typing.Optional[ValueSeed]] = {}
        self._register_tokens: typing.Set = set()

    @property
    def id(self) -> str:
        return self._id

    @property
    def value_ids(self) -> typing.List[str]:
        return list(self._get_available_value_ids())

    @property
    def alias_names(self) -> typing.List[str]:
        return list(self._get_available_aliases())

    # ======================================================================
    # main abstract methods

    @abc.abstractmethod
    def _register_value_and_data(self, value: Value, data: typing.Any) -> str:
        """Register data into this registry.

        Returns the values id. In case the value already has a value set (meaning it's different from string [NO_ID_YET_MARKER][kiara.data.value.NO_ID_YET_MARKER] / '__no_id_yet__'
        this method must use this id or throw an exception. Otherwise, it is required that the result id is different
        from ``NO_ID_YET_MARKER`` and a non-empty string.
        """

    @abc.abstractmethod
    def _register_remote_value(self, value: Value) -> typing.Optional[Value]:
        """Register an existing value from a different registry into this one.

        Arguments:
            value: the original value (with the '_registry' attribute still pointing to the original registry)

        Returns:
            either None (in which case the original value object will be copied with the '_registry' and '_kiara' attributes adjusted), or a value object.
        """

    @abc.abstractmethod
    def _get_available_value_ids(self) -> typing.Iterable[str]:
        """Return all of the registries available value ids."""

    @abc.abstractmethod
    def _get_value_obj_for_id(self, value_id: str) -> Value:
        pass

    @abc.abstractmethod
    def _get_value_data_for_id(self, value_item: str) -> typing.Any:
        pass

    @abc.abstractmethod
    def _get_available_aliases(self) -> typing.Iterable[str]:
        pass

    @abc.abstractmethod
    def _get_value_slot_for_alias(self, alias_name: str) -> ValueSlot:
        pass

    # -----------------------------------------------------------------------
    # main value retrieval methods

    def _create_value_obj(
        self,
        value_schema: ValueSchema,
        is_set: bool,
        is_none: bool,
        type_obj: typing.Optional[ValueType] = None,
        value_hashes: typing.Optional[typing.Mapping[str, ValueHash]] = None,
        metadata: typing.Optional[
            typing.Mapping[str, typing.Mapping[str, typing.Any]]
        ] = None,
    ):

        # if value_schema.is_constant and value_data not in [
        #     SpecialValue.NO_VALUE,
        #     SpecialValue.NOT_SET,
        #     None,
        # ]:
        #     raise Exception(
        #         "Can't create value. Is a constant, but value data was provided."
        #     )

        if type_obj is None:
            type_cls = self._kiara.get_value_type_cls(value_schema.type)
            type_obj = type_cls(**value_schema.type_config)

        register_token = uuid.uuid4()
        self._register_tokens.add(register_token)
        try:
            value = Value(
                registry=self,  # type: ignore
                value_schema=value_schema,
                type_obj=type_obj,  # type: ignore
                is_set=is_set,
                is_none=is_none,
                hashes=value_hashes,
                metadata=metadata,
                register_token=register_token,  # type: ignore
            )
            return value
        finally:
            self._register_tokens.remove(register_token)

    def _register_new_value_obj(
        self,
        value_obj: Value,
        value_data: typing.Any,
        value_seed: typing.Optional[ValueSeed],
        value_hashes: typing.Optional[typing.Mapping[str, ValueHash]] = None,
    ):

        # assert value_obj.id == NO_ID_YET_MARKER

        if value_data not in [
            SpecialValue.NO_VALUE,
            SpecialValue.NOT_SET,
            SpecialValue.IGNORE,
            None,
        ]:
            # TODO: should we keep the original value?
            value_data = value_obj.type_obj.import_value(value_data)

        value_id = self._register_value_and_data(value=value_obj, data=value_data)

        if value_obj.id == NO_ID_YET_MARKER:
            value_obj.id = value_id

        if value_obj.id != value_id:
            raise Exception(f"Inconsistent id for value: {value_obj.id} != {value_id}")

        if value_id not in self.value_ids:
            raise Exception(
                f"Value id '{value_id}' wasn't registered propertly in registry. This is most likely a bug."
            )

        if value_seed:
            self._seeds[value_id] = value_seed
        if value_hashes is None:
            value_hashes = {}
        for hash_type, value_hash in value_hashes.items():
            self._hashes.setdefault(hash_type, {})[value_hash.hash] = value_id

        return value_obj

    def register_data(
        self,
        value_data: typing.Any = SpecialValue.NOT_SET,
        value_schema: typing.Optional[ValueSchema] = None,
        value_seed: typing.Optional[ValueSeed] = None,
    ) -> Value:

        value_id = str(uuid.uuid4())

        if value_id in self.value_ids:
            raise Exception(
                f"Can't register value: value id '{value_id}' already exists."
            )

        if value_id in self.alias_names:
            raise Exception(
                f"Can't register value: value id '{value_id}' already exists as alias."
            )

        if value_schema is None:
            if isinstance(value_data, Value):
                value_schema = value_data.value_schema
            else:
                raise Exception(f"No value schema provided for value: {value_data}")

        cls = self._kiara.get_value_type_cls(value_schema.type)
        _type_obj = cls(**value_schema.type_config)

        if isinstance(value_data, Value):
            if value_data._registry == self and value_data.id in self.value_ids:
                # TODO: check it's really the same
                return value_data
            else:
                copied_value = self._register_remote_value(value_data)

                if value_data.id not in self.value_ids:
                    raise Exception(
                        f"Value with id '{value_data.id}' wasn't successfully registered. This is most likely a bug."
                    )

                if copied_value is None:
                    copied_value = value_data.copy()
                    copied_value._registry = self
                    copied_value._kiara = self._kiara
                else:
                    # TODO: make sure _registry and _kiara attributes are correct?
                    pass

                if value_data.id != copied_value.id:
                    raise Exception(
                        f"Imported value object with id '{value_data.id}' resulted in copied value with different id. This is a bug."
                    )

                for hash_type, value_hash in copied_value.get_hashes().items():
                    self._hashes.setdefault(hash_type, {})[value_hash.hash] = value_id
                self._seeds[copied_value.id] = value_data.get_value_seed()
                return copied_value

        existing_value: typing.Optional[Value] = None
        value_hashes = {}

        if value_data not in [None, SpecialValue.NOT_SET, SpecialValue.NO_VALUE]:
            for hash_type in _type_obj.get_supported_hash_types():
                hash_str = _type_obj.calculate_value_hash(
                    value=value_data, hash_type=hash_type
                )
                if hash_str in self._hashes.get(hash_type, {}).keys():
                    existing_value_id = self._hashes[hash_type][hash_str]
                    existing_value = self.get_value_obj(existing_value_id)
                    break
                else:
                    value_hashes[hash_type] = ValueHash(
                        hash=hash_str, hash_type=hash_type
                    )

        if existing_value:
            if existing_value.id in self._seeds.keys() and value_seed:
                raise NotImplementedError()

            # self._seeds[existing_value.id].append(value_seed)
            return existing_value

        if value_schema.is_constant:
            if value_data not in [None, SpecialValue.NOT_SET, SpecialValue.NO_VALUE]:
                raise NotImplementedError()
            value_data = value_schema.default

        is_set = value_data != SpecialValue.NOT_SET
        is_none = value_data in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]

        value = self._create_value_obj(
            value_schema=value_schema,
            is_set=is_set,
            is_none=is_none,
            type_obj=_type_obj,
            value_hashes=value_hashes,
        )

        self._register_new_value_obj(
            value_obj=value,
            value_data=value_data,
            value_seed=value_seed,
            value_hashes=value_hashes,
        )

        return value

    def _check_register_token(self, register_token: uuid.UUID):

        return register_token in self._register_tokens

    def _create_alias_obj(self, alias: str) -> ValueAlias:

        if alias.startswith("value:"):
            alias = alias[6:]
        value_alias = ValueAlias.from_string(value_alias=alias)

        if (
            value_alias.alias not in self._get_available_value_ids()
            and value_alias.alias not in self._get_available_aliases()
        ):
            raise Exception(
                f"Neither id nor alias '{alias}' registered with this registry."
            )

        if (
            value_alias.alias in self._get_available_aliases()
            and value_alias.tag is None
            and value_alias.version is None
        ):
            value_alias.version = self.get_latest_version_for_alias(value_alias.alias)

        return value_alias

    def get_value_obj(
        self, value_item: typing.Union[str, Value, ValueAlias, ValueSlot]
    ) -> typing.Optional[Value]:

        if value_item == NO_ID_YET_MARKER:
            raise Exception("Can't get value object: value not fully registered yet.")

        if isinstance(value_item, Value):
            value_item = value_item.id
        elif isinstance(value_item, ValueSlot):
            value_item = value_item.get_latest_value().id

        if isinstance(value_item, str):
            value_item = self._create_alias_obj(value_item)
        elif not isinstance(value_item, ValueAlias):
            raise TypeError(
                f"Invalid type '{type(value_item)}' for value item parameter."
            )

        if value_item.alias in self._get_available_value_ids():
            _value_id = value_item.alias
            _value: typing.Optional[Value] = self._get_value_obj_for_id(_value_id)
        elif value_item.alias in self._get_available_aliases():
            _value = self._resolve_alias_to_value(value_item)
        else:
            return None
            # raise Exception(
            #     f"No value or alias registered in registry with string: {value_item}"
            # )

        return _value

    def get_value_data(
        self, value_item: typing.Union[str, Value, ValueSlot, ValueAlias]
    ) -> typing.Any:

        value_obj = self.get_value_obj(value_item)
        if value_obj is None:
            raise Exception(f"No value registered for: {value_item}")

        if not value_obj.is_set and value_obj.value_schema.default not in (
            SpecialValue.NO_VALUE,
            SpecialValue.NOT_SET,
            None,
        ):
            return value_obj.value_schema.default
        elif not value_obj.is_set:
            # return None
            raise Exception("Value not set.")

        data = self._get_value_data_for_id(value_obj.id)
        if data == SpecialValue.NO_VALUE:
            return None
        elif isinstance(data, Value):
            return data.get_value_data()
        else:
            return data

    def get_value_seed(
        self, value_item: typing.Union[str, Value, ValueSlot]
    ) -> typing.Optional[ValueSeed]:

        value_obj = self.get_value_obj(value_item=value_item)
        if value_obj is None:
            raise Exception(f"No value registered for: {value_item}")
        return self._seeds.get(value_obj.id, None)

    # def get_value_info(self, value_item: typing.Union[str, Value, ValueAlias]) -> ValueInfo:
    #
    #     value_obj = self.get_value_obj(value_item=value_item)
    #
    #     aliases = self.find_aliases_for_value_id(value_id=value_obj.id, include_all_versions=True, include_tags=True)
    #
    #     info = ValueInfo(value_id=value_obj.id, value_type=value_obj.value_schema.type, aliases=aliases, metadata=value_obj.get_metadata())
    #     return info

    def get_value_slot(
        self, alias: typing.Union[str, ValueSlot]
    ) -> typing.Optional[ValueSlot]:

        if isinstance(alias, ValueSlot):
            alias = alias.id

        if alias not in self.alias_names:
            return None

        return self._get_value_slot_for_alias(alias_name=alias)

    # -----------------------------------------------------------------------
    def _resolve_hash_to_value(
        self, hash_str: str, hash_type: typing.Optional[str] = None
    ) -> typing.Optional[Value]:

        matches: typing.Set[str] = set()
        if hash_type is None:
            for hash_type, details in self._hashes.items():
                if hash_str in details.keys():
                    matches.add(details[hash_str])
        else:
            hashes_for_type = self._hashes.get(hash_type, {})
            if hash_str in hashes_for_type.keys():
                matches.add(hashes_for_type[hash_str])

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception(f"Multiple values found for hash '{hash_str}'.")

        match_id = next(iter(matches))
        value = self.get_value_obj(match_id)
        return value

    # alias-related methods
    def _resolve_alias_to_value(self, alias: ValueAlias) -> typing.Optional[Value]:

        alias_name = alias.alias
        alias_version = alias.version
        alias_tag = alias.tag

        value_slot = self.get_value_slot(alias_name)
        if not value_slot:
            raise Exception(f"No alias '{alias_name}' registered with registry.")

        if alias_tag:
            _version = value_slot.tags.get(alias_tag)
            if alias_version:
                if _version != alias_version:
                    raise Exception(
                        f"Value alias object contains both tag and version information, but actual version of tag resolves to different version in the registry: {_version} != {alias_version}"
                    )
            else:
                alias_version = _version

        assert alias_version is not None
        value = value_slot.values.get(alias_version, None)
        return value

    def _find_aliases_for_value_id(self, value_id: str) -> typing.List[ValueAlias]:
        """Find all aliases that point to the specified value id.

        Sub-classes may overwrite this method for performance reasons.
        """

        result = []
        for alias in self._get_available_aliases():
            value_slot = self.get_value_slot(alias)
            assert value_slot
            aliases = value_slot.find_linked_aliases(value_id)
            result.extend(aliases)

        return result

    def _get_tags_for_alias(self, alias: str) -> typing.Iterable[str]:

        value_slot = self.get_value_slot(alias)
        if not value_slot:
            raise Exception(f"No alias '{alias}' registered with registry.")

        return value_slot.tags.keys()

    def find_aliases_for_value_id(
        self,
        value_id: str,
        include_all_versions: bool = False,
        include_tags: bool = False,
    ) -> typing.List[ValueAlias]:

        aliases = self._find_aliases_for_value_id(value_id=value_id)
        result = []
        latest_cache: typing.Dict[str, int] = {}
        for alias in aliases:

            if alias.version is not None:
                if not include_all_versions:
                    if alias.alias in latest_cache.keys():
                        latest_version = latest_cache[alias.alias]
                    else:
                        latest_version = self.get_latest_version_for_alias(alias.alias)
                        latest_cache[alias.alias] = latest_version
                    if latest_version == alias.version:
                        result.append(alias)
                else:
                    result.append(alias)

            if alias.tag is not None and include_tags:
                result.append(alias)

        return result

    def get_latest_version_for_alias(self, alias: str) -> int:

        versions = self.get_versions_for_alias(alias)
        if not versions:
            return 0
        else:
            return max(versions)

    def get_versions_for_alias(self, alias: str) -> typing.Iterable[int]:
        """Return all available versions for the specified alias."""

        if isinstance(alias, str):
            _alias = ValueAlias.from_string(alias)
        elif not isinstance(alias, ValueAlias):
            raise TypeError(f"Invalid type for alias: {type(alias)}")
        else:
            _alias = alias

        value_slot = self.get_value_slot(alias)
        if not value_slot:
            raise Exception(f"No alias '{_alias.alias}' registered with registry.")

        return sorted(value_slot.values.keys())

    def get_tags_for_alias(self, alias: str) -> typing.Iterable[str]:

        if isinstance(alias, str):
            _alias = ValueAlias.from_string(alias)
        elif not isinstance(alias, ValueAlias):
            raise TypeError(f"Invalid type for alias: {type(alias)}")
        else:
            _alias = alias

        value_slot = self.get_value_slot(alias)
        if not value_slot:
            raise Exception(f"No alias '{_alias.alias}' registered with registry.")

        return sorted(value_slot.tags.keys())

    def __eq__(self, other):

        # TODO: compare all attributes if id is equal, just to make sure...

        if not isinstance(other, DataRegistry):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):

        return f"{self.__class__.__name__}(id={self.id}"

    def __str__(self):

        return self.__repr__()


VALID_ALIAS_PATTERN = re.compile("^[A-Za-z0-9_-]*$")


class DataRegistry(BaseDataRegistry):
    @abc.abstractmethod
    def _register_alias(self, alias_name: str, value_schema: ValueSchema) -> ValueSlot:
        pass

    def register_aliases(
        self,
        value_or_schema: typing.Union[Value, ValueSchema, str],
        aliases: typing.Iterable[str],
        callbacks: typing.Optional[typing.Iterable[ValueSlotUpdateHandler]] = None,
    ) -> typing.Mapping[str, ValueSlot]:

        result = {}
        for alias in aliases:
            vs = self.register_alias(
                value_or_schema=value_or_schema, alias_name=alias, callbacks=callbacks
            )
            result[alias] = vs

        return result

    def register_alias(
        self,
        value_or_schema: typing.Union[Value, ValueSchema, str],
        # preseed: bool = False,
        alias_name: typing.Optional[str] = None,
        callbacks: typing.Optional[typing.Iterable[ValueSlotUpdateHandler]] = None,
    ) -> ValueSlot:
        """Register a value slot.

        A value slot is an object that holds multiple versions of values that all use the same schema.

        Arguments:
            value_or_schema: a value, value_id, or schema, if value, it is added to the newly created slot (after preseed, if selected)
            preseed: whether to add an empty/non-set value to the newly created slot
            alias_name: the alias name, will be auto-created if not provided
            callbacks: a list of callbacks to trigger whenever the alias is updated
        """

        if alias_name is None:
            alias_name = str(uuid.uuid4())

        if not alias_name:
            raise Exception("Empty alias name not allowed.")

        match = bool(re.match(VALID_ALIAS_PATTERN, alias_name))
        if not match:
            raise Exception(
                f"Invalid alias '{alias_name}': only alphanumeric characters, '-', and '_' allowed in alias name."
            )

        # if the provided value_or_schema argument is a string, it must be a value_id (for now)
        if isinstance(value_or_schema, str):
            if value_or_schema not in self.value_ids:
                raise Exception(
                    f"Can't register alias '{alias_name}', provided string '{value_or_schema}' not an existing value id."
                )
            _value_or_schema = self.get_value_obj(value_or_schema)
            if _value_or_schema is None:
                raise Exception(f"No value registered for: {value_or_schema}")
            value_or_schema = _value_or_schema

        if isinstance(value_or_schema, Value):
            _value_schema = value_or_schema.value_schema
            _value: typing.Optional[Value] = value_or_schema
        elif isinstance(value_or_schema, ValueSchema):
            _value_schema = value_or_schema
            _value = None
        else:
            raise TypeError(f"Invalid value type: {type(value_or_schema)}")

        if alias_name in self._get_available_aliases():
            raise Exception(
                f"Can't register alias: alias '{alias_name}' already exists."
            )
        elif alias_name in self._get_available_value_ids():
            raise Exception(
                f"Can't register alias: alias '{alias_name}' already exists as value id."
            )

        # vs = ValueSlot.from_value(id=_id, value=value_or_schema)
        vs = self._register_alias(alias_name=alias_name, value_schema=_value_schema)
        # self._value_slots[vs.id] = vs

        # if preseed:
        #     _v = Value(value_schema=_value_schema, kiara=self._kiara, registry=self)
        #     vs.add_value(_v)

        if callbacks:
            vs.register_callbacks(*callbacks)
            # self.register_callbacks(vs, *callbacks)

        if _value is not None:
            vs.add_value(_value)

        return vs

    # def find_value_slots(
    #     self, value_item: typing.Union[str, Value]
    # ) -> typing.List[ValueSlot]:
    #
    #     value_item = self.get_value_obj(value_item)
    #     result = []
    #     for slot_id, slot in self._value_slots.items():
    #         if slot.is_latest_value(value_item):
    #             result.append(slot)
    #     return result

    # def register_callbacks(
    #     self,
    #     alias: typing.Union[str, ValueSlot],
    #     *callbacks: ValueSlotUpdateHandler,
    # ) -> None:
    #
    #     _value_slot = self.get_value_slot(alias)
    #     _value_slot.register_callbacks(*callbacks)

    def update_value_slot(
        self,
        value_slot: typing.Union[str, Value, ValueSlot],
        data: typing.Any,
        value_seed: typing.Optional[ValueSeed] = None,
    ) -> bool:

        # first, resolve a potential string into a value_slot or value
        if isinstance(value_slot, str):
            if value_slot in self.alias_names:
                _value_slot: typing.Union[
                    None, Value, ValueSlot, str
                ] = self.get_value_slot(alias=value_slot)
            elif value_slot in self.value_ids:
                _value_slot = self.get_value_obj(value_slot)
            else:
                _value_slot = value_slot

            if _value_slot is None:
                raise Exception(f"Can't retrieve target object for id: {value_slot}")
            value_slot = _value_slot

        if isinstance(value_slot, Value):
            aliases = self.find_aliases_for_value_id(value_slot.id)
            # slots = self.find_value_slots(value_slot)
            if len(aliases) == 0:
                raise Exception(f"No value slot found for value '{value_slot.id}'.")
            elif len(aliases) > 1:
                raise Exception(
                    f"Multiple value slots found for value '{value_slot.id}'. This is not supported (yet)."
                )
            _value_slot_2: typing.Optional[ValueSlot] = self.get_value_slot(
                alias=aliases[0].alias
            )
        elif isinstance(value_slot, ValueSlot):
            _value_slot_2 = value_slot
        else:
            raise TypeError(f"Invalid type for value slot: {type(value_slot)}")

        assert _value_slot_2 is not None

        if isinstance(data, Value):
            if value_seed:
                raise Exception("Can't update value slot with new value seed data.")
            _value: Value = data
        else:
            _value = self.register_data(
                value_data=data,
                value_schema=value_slot.value_schema,
                value_seed=value_seed,
            )
            # _value = self.create_value(
            #     value_data=data,
            #     value_schema=_value_slot.value_schema,
            #     value_seed=value_seed,
            # )

        return self._update_value_slot(
            value_slot=_value_slot_2, new_value=_value, trigger_callbacks=True
        )

    def update_value_slots(
        self, updated_values: typing.Mapping[typing.Union[str, ValueSlot], typing.Any]
    ) -> typing.Mapping[ValueSlot, bool]:

        updated: typing.Dict[str, typing.List[ValueSlot]] = {}
        cb_map: typing.Dict[str, ValueSlotUpdateHandler] = {}

        result = {}

        invalid: typing.Set[str] = set()
        for alias, value_item in updated_values.items():
            if isinstance(alias, ValueSlot):
                alias = alias.id

            _alias = self.get_value_slot(alias)
            if _alias is None:
                if isinstance(alias, ValueSlot):
                    invalid.add(alias.id)
                else:
                    invalid.add(alias)

        if invalid:
            raise Exception(
                f"Can't update value slots: invalid alias name(s) '{', '.join(invalid)}'."
            )

        for alias, value_item in updated_values.items():

            if isinstance(alias, ValueSlot):
                alias = alias.id

            value_slot = self.get_value_slot(alias)

            if value_slot is None:
                raise Exception(f"Can't retrieve value slot for alias: {alias}")

            if isinstance(value_item, Value):
                _value_item: Value = value_item
                if _value_item._registry != self:
                    raise NotImplementedError()
            else:
                _value_item = self.register_data(
                    value_data=value_item, value_schema=value_slot.value_schema
                )

            updated_item = self._update_value_slot(
                value_slot=value_slot, new_value=_value_item, trigger_callbacks=False
            )
            result[value_slot] = updated_item
            if updated_item:
                for cb_id, cb in value_slot._callbacks.items():
                    cb_map[cb_id] = cb
                    updated.setdefault(cb_id, []).append(value_slot)

        for cb_id, value_slots in updated.items():
            cb = cb_map[cb_id]
            cb.values_updated(*value_slots)

        return result

    def _update_value_slot(
        self, value_slot: ValueSlot, new_value: Value, trigger_callbacks: bool = True
    ) -> bool:

        last_version = value_slot.latest_version_nr
        new_version = value_slot.add_value(
            new_value, trigger_callbacks=trigger_callbacks
        )

        updated = last_version != new_version
        return updated


class InMemoryDataRegistry(DataRegistry):
    def __init__(self, kiara: "Kiara"):

        self._values: typing.Dict[str, Value] = {}
        self._value_data: typing.Dict[str, typing.Any] = {}
        self._value_slots: typing.Dict[str, ValueSlot] = {}

        super().__init__(kiara=kiara)

    def _register_value_and_data(self, value: Value, data: typing.Any) -> str:

        value_id = str(uuid.uuid4())
        self._values[value_id] = value
        self._value_data[value_id] = data

        return value_id

    def _register_remote_value(self, value: Value) -> None:

        raise NotImplementedError()

    def _get_available_value_ids(self) -> typing.Iterable[str]:

        return self._values.keys()

    def _get_value_data_for_id(self, value_id: str) -> typing.Any:

        return self._value_data[value_id]

    def _get_value_obj_for_id(self, value_id: str) -> Value:

        return self._values[value_id]

    def _get_value_slot_for_alias(self, alias_name: str) -> ValueSlot:

        return self._value_slots[alias_name]

    def _get_available_aliases(self) -> typing.Iterable[str]:

        return self._value_slots.keys()

    def _register_alias(self, alias_name: str, value_schema: ValueSchema) -> ValueSlot:

        vs = ValueSlot(
            id=alias_name, value_schema=value_schema, kiara=self._kiara, registry=self
        )
        self._value_slots[alias_name] = vs

        return vs
