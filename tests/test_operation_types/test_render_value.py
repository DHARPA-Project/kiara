# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from click import Group

from kiara.context import Kiara
from kiara.utils.cli import terminal_print


def test_render_string(kiara: Kiara):
    value = kiara.data_registry.register_data("xxx", schema="string")
    pp_result = kiara.data_registry.pretty_print_data(value_id=value.value_id)

    assert pp_result == "xxx"


def test_render_integer(kiara: Kiara):
    value = kiara.data_registry.register_data(100, schema="integer")
    pp_result = kiara.data_registry.pretty_print_data(value_id=value.value_id)
    assert pp_result == "100"


def test_render_float(kiara: Kiara):
    value = kiara.data_registry.register_data(100.01, schema="float")
    pp_result = kiara.data_registry.pretty_print_data(value_id=value.value_id)
    assert pp_result == "100.01"


def test_render_available_data(preseeded_data_store: Kiara):

    for (
        value_id
    ) in preseeded_data_store.data_registry.retrieve_all_available_value_ids():
        value = preseeded_data_store.data_registry.get_value(value_id)
        pp_result = preseeded_data_store.data_registry.pretty_print_data(
            value_id=value.value_id
        )
        terminal_print(Group(pp_result))
