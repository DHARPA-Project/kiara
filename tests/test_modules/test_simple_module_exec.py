# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import pytest

from kiara.context import Kiara
from kiara.exceptions import InvalidValuesException


def test_module_processing(kiara: Kiara):

    and_mod = kiara.create_manifest("logic.and")

    # boolean_schema = ValueSchema(type="boolean")
    inputs = {"a": None, "b": True}
    with pytest.raises(InvalidValuesException) as e:
        kiara.process(manifest=and_mod, inputs=inputs)

    assert "a" in e.value.invalid_inputs.keys()
    assert "not set" in e.value.invalid_inputs["a"]

    inputs = {"a": True, "b": True}
    outputs = kiara.process(manifest=and_mod, inputs=inputs)

    assert outputs.get_value_data("y") is True

    inputs = {"a": False, "b": True}
    outputs = kiara.process(manifest=and_mod, inputs=inputs)
    assert outputs.get_value_data("y") is False
