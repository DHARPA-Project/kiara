# -*- coding: utf-8 -*-

from kiara.api import Kiara, ValueSchema
from kiara.defaults import SpecialValue

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_values_create(kiara: Kiara):

    value_schema = ValueSchema(type="string")
    v = kiara.data_registry.register_data(data=None, schema=value_schema)
    assert not v.is_set


def test_registry_values(kiara: Kiara):

    value_schema_1 = ValueSchema(type="string", optional=True)

    reg = kiara.data_registry

    v = reg.register_data(data=None, schema=value_schema_1)

    v = reg.register_data(data=None, schema=value_schema_1)
    assert v.data is None

    v = reg.register_data(data=SpecialValue.NO_VALUE, schema=value_schema_1)
    assert v.data is None

    v = reg.register_data(data=SpecialValue.NOT_SET, schema=value_schema_1)
    assert v.data is None
