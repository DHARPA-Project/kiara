# -*- coding: utf-8 -*-
from kiara import KiaraAPI

#  Copyright (c) 2023, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_operation_list(api: KiaraAPI):

    op_list = api.list_operation_ids()

    assert "logic.and" in op_list
    assert "logic.xor" in op_list
    assert "query.database" in op_list
    assert "render.database.as.string" not in op_list

    op_list = api.list_operation_ids(include_internal=True)
    assert "render.database.as.string" in op_list

    op_list = api.list_operation_ids("query.database")
    assert "query.database" in op_list


def test_get_operation(api: KiaraAPI):

    op = api.get_operation("query.database")
    assert "Execute a sql query against a (sqlite) database." in op.doc.full_doc

    op = api.get_operation("logic.and")
    result = op.run(kiara=api.context, inputs={"a": True, "b": True})

    assert result.get_value_data("y") is True

    op = api.get_operation("logic.nand")
    result = op.run(kiara=api.context, inputs={"a": True, "b": True})

    assert result.get_value_data("y") is False
