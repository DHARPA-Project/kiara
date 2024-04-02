# -*- coding: utf-8 -*-
from kiara.interfaces.python_api.base_api import BaseAPI

#  Copyright (c) 2023, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_module_types(api: BaseAPI):
    # test 'boolean' and 'table' are in the available data type names

    assert "logic.and" in api.list_module_type_names()
    assert "query.database" in api.list_module_type_names()


def test_module_type_info(api: BaseAPI):

    infos = api.retrieve_module_types_info(filter="query.databas")
    assert len(infos.item_infos) == 1

    info = api.retrieve_module_type_info("query.database")
    assert info == next(iter(infos.item_infos.values()))

    assert info.type_name == "query.database"
