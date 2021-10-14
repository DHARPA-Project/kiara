# -*- coding: utf-8 -*-

from kiara import Kiara
from kiara.operations.calculate_hash import CalculateHashOperationType


def test_calculate_hash_for_all_available_data(preseeded_data_store: Kiara):

    hash_ops: CalculateHashOperationType = (
        preseeded_data_store.operation_mgmt.operation_types["calculate_hash"]
    )

    for value_id in preseeded_data_store.data_store.value_ids:
        value = preseeded_data_store.data_store.get_value_obj(value_id)

        ops = hash_ops.get_hash_operations_for_type(value.type_name)
        for op in ops.values():

            inputs = {value.type_name: value}
            result = op.run(**inputs)
            assert isinstance(result.get_value_data("hash"), str)
