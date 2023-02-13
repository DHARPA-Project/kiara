# -*- coding: utf-8 -*-
from kiara.api import KiaraAPI

#  Copyright (c) 2023, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_data_types(api: KiaraAPI):
    # test 'boolean' and 'table' are in the available data type names

    assert "boolean" in api.list_data_type_names()
    assert "table" in api.list_data_type_names()


def test_internal_data_types(api: KiaraAPI):

    assert not api.is_internal_data_type("table")
    assert not api.is_internal_data_type("boolean")

    assert api.is_internal_data_type("render_value_result")


def test_data_type_info(api: KiaraAPI):

    infos = api.retrieve_data_types_info(filter="table")
    assert len(infos.item_infos) == 1

    info = api.retrieve_data_type_info("table")
    assert info == next(iter(infos.item_infos.values()))

    assert info.type_name == "table"
