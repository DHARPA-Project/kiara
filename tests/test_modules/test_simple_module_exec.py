# -*- coding: utf-8 -*-
import pytest

from kiara import Kiara
from kiara.data.values import Value, ValueSchema


def test_simple_module_exec(kiara: Kiara):

    and_mod = kiara.create_module("logic.and")

    boolean_schema = ValueSchema(type="boolean")
    inp_a = Value(value_schema=boolean_schema, kiara=kiara)
    inp_b = Value(value_schema=boolean_schema, kiara=kiara)

    inputs = {"a": inp_a, "b": inp_b}
    with pytest.raises(Exception) as e:
        and_mod.run(**inputs)

    assert "Inputs not valid" in str(e.value)

    inp_a = Value(value_data=True, value_schema=boolean_schema, kiara=kiara)
    inp_b = Value(value_data=True, value_schema=boolean_schema, kiara=kiara)
    inputs = {"a": inp_a, "b": inp_b}
    outputs = and_mod.run(**inputs)
    assert outputs.get_value_data("y") is True

    inp_b_2 = Value(value_data=False, value_schema=boolean_schema, kiara=kiara)
    inputs = {"a": inp_a, "b": inp_b_2}
    outputs = and_mod.run(**inputs)
    assert outputs.get_value_data("y") is False
