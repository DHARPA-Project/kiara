# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import pytest

import typing

from kiara import Kiara
from kiara.data.registry import DataRegistry, InMemoryDataRegistry
from kiara.data.values import NO_ID_YET_MARKER, Value, ValueSchema, ValueSlot


class SimpleTestRegistry(DataRegistry):
    """Test implementation of read-only registry, which uses parts of the value data as id."""

    def __init__(self, kiara: Kiara):

        self._values: typing.Dict[str, Value] = {}
        self._data: typing.Dict[str, typing.Any] = {}
        self._value_slots: typing.Dict[str, ValueSlot] = {}
        super().__init__(kiara=kiara)

    def _register_value_and_data(self, value: Value, data: typing.Any) -> str:

        assert isinstance(data, str)
        _id, aliases = data.split(".", maxsplit=1)

        if _id in self._values.keys():
            raise Exception(f"Duplicate id: {_id}")

        self._values[_id] = value
        self._data[_id] = data
        if aliases:
            for alias in aliases.split("."):
                if alias not in self._value_slots:
                    value_slot = ValueSlot(
                        id=alias,
                        value_schema=value.value_schema,
                        kiara=self._kiara,
                        registry=self,
                    )
                    self._value_slots[alias] = value_slot
                else:
                    value_slot = self._value_slots[alias]
                value_slot.add_value(value)
        return _id

    def _register_remote_value(self, value: Value) -> typing.Optional[Value]:
        raise NotImplementedError()

    def _get_available_value_ids(self) -> typing.Iterable[str]:
        return self._values.keys()

    def _get_value_obj_for_id(self, value_id: str) -> Value:
        return self._values[value_id]

    def _get_value_data_for_id(self, value_item: str) -> typing.Any:
        return self._data[value_item]

    def _get_available_aliases(self) -> typing.Iterable[str]:
        return self._value_slots.keys()

    def _get_value_slot_for_alias(self, alias_name: str) -> ValueSlot:
        return self._value_slots.get(alias_name, None)

    def _register_alias(self, alias_name: str, value_schema: ValueSchema) -> ValueSlot:
        raise NotImplementedError()


def test_read_registry_subclass(kiara: Kiara):

    reg = SimpleTestRegistry(kiara=kiara)

    string_schema = ValueSchema(type="string")
    value = reg.register_data(value_data="xxx.", value_schema=string_schema)
    assert value.id == "xxx"

    assert set(reg.alias_names) == set()

    with pytest.raises(Exception) as exc_info:
        reg.register_data(value_data="xxx.", value_schema=string_schema)

    assert "Duplicate" in str(exc_info.value)

    value_2 = reg.register_data(
        value_data="yyy.alias_1.alias_2", value_schema=string_schema
    )
    assert value_2.id == "yyy"
    assert set(reg.alias_names) == set(["alias_1", "alias_2"])

    assert len(reg.find_aliases_for_value("yyy")) == 2
    assert reg.get_versions_for_alias("alias_1") == [1]

    value_3 = reg.register_data(
        value_data="zzz.alias_1.alias_2", value_schema=string_schema
    )
    assert value_3.id == "zzz"
    assert reg.get_versions_for_alias("alias_1") == [1, 2]

    latest_alias_1_val = reg.get_value_obj("alias_1")
    assert latest_alias_1_val.id == "zzz"
    assert latest_alias_1_val.get_value_data() == "zzz.alias_1.alias_2"

    value_2_stored = reg.get_value_obj("alias_1@1")
    assert value_2 == value_2_stored
    assert value_2.get_value_data() == value_2_stored.get_value_data()


def test_default_data_registry(kiara: Kiara):

    reg = InMemoryDataRegistry(kiara=kiara)

    string_schema = ValueSchema(type="string")
    value_1 = reg.register_data("xxx", value_schema=string_schema)
    assert value_1.id
    assert value_1.id != NO_ID_YET_MARKER

    val_ret = reg.get_value_obj(value_1.id)
    assert val_ret == value_1
    assert val_ret.get_value_data() == value_1.get_value_data()

    assert not reg.alias_names
    assert value_1.id in reg.value_ids
    assert len(reg.value_ids) == 1

    value_2 = reg.register_data("xxx", value_schema=string_schema)
    assert value_2.get_value_data() == value_1.get_value_data()
    assert value_1.id != value_2.id

    assert len(reg.value_ids) == 2


def test_default_data_registry_aliases(kiara: Kiara):

    reg = InMemoryDataRegistry(kiara=kiara)

    string_schema = ValueSchema(type="string")
    value_slot_1 = reg.register_alias(value_or_schema=string_schema, alias_name="xxx")
    assert len(reg.alias_names) == 1
    assert "xxx" in reg.alias_names

    with pytest.raises(Exception) as exc_info:
        value_slot_1.get_latest_value()

    assert "No value added" in str(exc_info.value)

    value_ret = reg.get_value_obj("xxx")
    assert value_ret is None

    val_1 = reg.register_data(value_data="xxx", value_schema=string_schema)
    value_slot_1.add_value(val_1)

    assert value_slot_1.get_latest_value().get_value_data() == "xxx"

    assert val_1.id == value_slot_1.get_latest_value().id

    reg.register_alias(value_or_schema=string_schema, alias_name="yyy")
    aliases = reg.find_aliases_for_value(val_1.id)
    assert len(aliases) == 1
    assert aliases[0].alias == "xxx"
