# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from rich.console import RenderGroup

from kiara import Kiara
from kiara.utils.output import rich_print


def test_pretty_print_string(kiara: Kiara):
    value = kiara.data_registry.register_data("xxx", value_schema="string")
    pp_result = kiara.pretty_print(value=value)
    assert pp_result == ["xxx"]


def test_pretty_print_integer(kiara: Kiara):
    value = kiara.data_registry.register_data(100, value_schema="integer")
    pp_result = kiara.pretty_print(value=value)
    assert pp_result == ["100"]


def test_pretty_print_float(kiara: Kiara):
    value = kiara.data_registry.register_data(100.01, value_schema="float")
    pp_result = kiara.pretty_print(value=value)
    assert pp_result == ["100.01"]


def test_pretty_print_all_available_data(preseeded_data_store: Kiara):

    for value_id in preseeded_data_store.data_store.value_ids:
        value = preseeded_data_store.data_store.get_value_obj(value_id)
        pp_result = preseeded_data_store.pretty_print(value=value)
        rich_print(RenderGroup(*pp_result))
