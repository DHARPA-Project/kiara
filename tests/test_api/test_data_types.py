# -*- coding: utf-8 -*-
from kiara.interfaces.python_api.base_api import BaseAPI

#  Copyright (c) 2023, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_data_types(api: BaseAPI):
    # test 'boolean' and 'table' are in the available data type names

    assert "boolean" in api.list_data_type_names()
    assert "table" in api.list_data_type_names()


def test_internal_data_types(api: BaseAPI):

    assert not api.is_internal_data_type("table")
    assert not api.is_internal_data_type("boolean")

    assert api.is_internal_data_type("render_value_result")


def test_data_type_info(api: BaseAPI):

    infos = api.retrieve_data_types_info(filter="table")
    assert len(infos.item_infos) == 2

    info = api.retrieve_data_type_info("table")
    infos = iter(infos.item_infos.values())
    try:
        assert info == next(infos)
    except Exception:
        assert info == next(infos)

    assert info.type_name in ["table", "tables"]
