# -*- coding: utf-8 -*-

from kiara import Kiara


def test_operation_type_list(kiara: Kiara):

    assert "calculate_hash" in kiara.operation_mgmt.operation_types.keys()

    ch_ops = kiara.operation_mgmt.get_operations("calculate_hash")
    ch_tm = ch_ops.get_type_metadata()
    assert ch_tm.type_name == "calculate_hash"
