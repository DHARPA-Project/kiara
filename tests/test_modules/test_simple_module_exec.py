# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import pytest

from kiara import Kiara


def test_simple_module_exec(kiara: Kiara):

    and_mod = kiara.create_module("logic.and")

    # boolean_schema = ValueSchema(type="boolean")

    inputs = {"a": None, "b": True}
    with pytest.raises(Exception) as e:
        and_mod.run(**inputs)

    assert "input field(s) not valid" in str(e.value)

    # inp_a = Value(value_data=True, value_schema=boolean_schema, kiara=kiara)
    # inp_b = Value(value_data=True, value_schema=boolean_schema, kiara=kiara)
    inputs = {"a": True, "b": True}
    outputs = and_mod.run(**inputs)
    assert outputs.get_value_data("y") is True

    inputs = {"a": False, "b": True}
    outputs = and_mod.run(**inputs)
    assert outputs.get_value_data("y") is False
