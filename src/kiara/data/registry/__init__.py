# -*- coding: utf-8 -*-
import abc
import typing
import uuid

from kiara.data import Value
from kiara.data.values import ValueSchema, ValueSeed, ValueSlot
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


class DataRegistry(abc.ABC):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._value_slots: typing.Dict[str, ValueSlot] = {}

    @abc.abstractmethod
    def _register_value(self, value: Value, data: typing.Any) -> None:
        pass

    @abc.abstractmethod
    def _get_value_data_for_id(self, value_item: str) -> typing.Any:
        pass

    @abc.abstractmethod
    def _get_value_for_id(self, value_id: str) -> Value:
        pass

    @abc.abstractmethod
    def _get_available_value_ids(self) -> typing.Iterable[str]:
        """Return all of the registries available value ids."""

    def get_value_data(
        self, value_item: typing.Union[str, Value, ValueSlot]
    ) -> typing.Any:

        value_obj = self.get_value(value_item)

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

    def get_value(self, value_item: typing.Union[str, Value, ValueSlot]) -> Value:

        if isinstance(value_item, Value):
            _value_id = value_item.id
        elif isinstance(value_item, ValueSlot):
            vs = self.get_value_slot(
                value_slot=value_item
            )  # just to make sure this slot is valid
            _value_obj = vs.get_latest_value()
            _value_id = _value_obj.id
        elif isinstance(value_item, str):
            if value_item in self._get_available_value_ids():
                _value_id = value_item
            elif value_item in self._value_slots.keys():
                _value_slot = self.get_value_slot(value_item)
                _value_obj = _value_slot.get_latest_value()
                _value_id = _value_obj.id
            else:
                raise Exception(
                    f"No value or value_slot id registered in registry: {value_item}"
                )
        else:
            raise TypeError(f"Invalid type for value item: {type(value_item)}")

        return self._get_value_for_id(_value_id)

    def get_value_slot(self, value_slot: typing.Union[str, ValueSlot]) -> ValueSlot:

        if isinstance(value_slot, ValueSlot):
            _value_slot = value_slot.id
        elif isinstance(value_slot, str):
            _value_slot = value_slot
        else:
            raise TypeError(f"Invalid type for value item: {type(value_slot)}")

        if _value_slot not in self._value_slots.keys():
            raise KeyError(f"No value_slot with id '{_value_slot}' registered.")

        return self._value_slots[_value_slot]

    def create_value(
        self,
        value_data: typing.Any = SpecialValue.NOT_SET,
        value_schema: typing.Optional[ValueSchema] = None,
        is_constant: typing.Optional[bool] = False,
        value_seed: typing.Optional[ValueSeed] = None,
        register_slot: bool = False,
        callbacks: typing.Optional[typing.Iterable[ValueSlotUpdateHandler]] = None,
    ):

        if not register_slot and callbacks:
            raise Exception(
                "Callbacks can only be registered on slots, so 'register_slot' must be set to 'True'."
            )

        value = Value(  # type: ignore
            value_data=value_data,  # type: ignore
            value_schema=value_schema,
            is_constant=is_constant,
            value_seed=value_seed,
            kiara=self._kiara,  # type: ignore
            registry=self,  # type: ignore
        )
        assert value.id in self._get_available_value_ids()

        if register_slot:
            self.register_slot(value, callbacks=callbacks)

        return value

    def register_slot(
        self,
        value_or_schema: typing.Union[Value, ValueSchema],
        callbacks: typing.Optional[typing.Iterable[ValueSlotUpdateHandler]] = None,
    ) -> ValueSlot:
        """Register a value slot.

        A value slot is an object that holds multiple versions of values that all use the same schema.
        """

        id = str(uuid.uuid4())
        if isinstance(value_or_schema, ValueSchema):
            value_or_schema = Value(
                value_data=SpecialValue.NOT_SET,  # type: ignore
                value_schema=value_or_schema,
                kiara=self._kiara,  # type: ignore
                registry=self,  # type: ignore
            )
        elif not isinstance(value_or_schema, Value):
            raise TypeError(f"Invalid value type: {type(value_or_schema)}")

        vs = ValueSlot.from_value(id=id, value=value_or_schema)
        self._value_slots[vs.id] = vs
        if callbacks:
            self.register_callbacks(vs, *callbacks)

        return vs

    def register_callbacks(
        self,
        value_slot: typing.Union[str, ValueSlot],
        *callbacks: ValueSlotUpdateHandler,
    ) -> None:

        _value_slot = self.get_value_slot(value_slot)
        _value_slot.register_callbacks(*callbacks)

    def find_value_slots(
        self, value_item: typing.Union[str, Value]
    ) -> typing.List[ValueSlot]:

        value_item = self.get_value(value_item)
        result = []
        for slot_id, slot in self._value_slots.items():
            if slot.is_latest_value(value_item):
                result.append(slot)
        return result

    def update_value_slot(
        self,
        value_slot: typing.Union[str, Value, ValueSlot],
        data: typing.Any,
        value_seed: typing.Optional[ValueSeed] = None,
    ) -> bool:

        if isinstance(value_slot, str):
            if value_slot in self._value_slots.keys():
                value_slot = self.get_value_slot(value_slot)
            elif value_slot in self._get_available_value_ids():
                value_slot = self.get_value(value_slot)

        if isinstance(value_slot, Value):
            slots = self.find_value_slots(value_slot)
            if len(slots) == 0:
                raise Exception(f"No value slot found for value '{value_slot.id}'.")
            elif len(slots) > 1:
                raise Exception(
                    f"Multiple value slots found for value '{value_slot.id}'. This is not supported (yet)."
                )
            _value_slot: ValueSlot = slots[0]
        elif isinstance(value_slot, ValueSlot):
            _value_slot = value_slot
        else:
            raise TypeError(f"Invalid type for value slot: {type(value_slot)}")

        if isinstance(data, Value):
            if value_seed:
                raise Exception("Can't update value slot with new value seed data.")
            _value: Value = data
        else:
            _value = self.create_value(
                value_data=data,
                value_schema=_value_slot.value_schema,
                value_seed=value_seed,
            )

        return self._update_value_slot(
            value_slot=_value_slot, new_value=_value, trigger_callbacks=True
        )

    def update_value_slots(
        self, updated_values: typing.Mapping[typing.Union[str, ValueSlot], typing.Any]
    ) -> typing.Mapping[ValueSlot, bool]:

        updated: typing.Dict[str, typing.List[ValueSlot]] = {}
        cb_map: typing.Dict[str, ValueSlotUpdateHandler] = {}

        result = {}

        for value_slot, value_item in updated_values.items():

            value_slot = self.get_value_slot(value_slot)
            if isinstance(value_item, Value):
                _value_item: Value = value_item
            else:
                _value_item = self.create_value(
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
        super().__init__(kiara=kiara)

    def _get_available_value_ids(self) -> typing.Iterable[str]:

        return self._values.keys()

    def _get_value_for_id(self, value_id: str) -> Value:

        return self._values[value_id]

    def _get_value_data_for_id(self, value_id: str) -> typing.Any:

        return self._value_data[value_id]

    def _register_value(self, value: Value, data: typing.Any) -> None:

        self._values[value.id] = value
        self._value_data[value.id] = data
