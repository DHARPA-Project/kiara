# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# def test_sample_data_table(preseeded_data_store: Kiara):
#
#     hash_ops: SampleValueOperationType = (
#         preseeded_data_store.operation_mgmt.operation_types["sample"]
#     )
#
#     nodes_table = preseeded_data_store.get_value("journal_nodes_table")
#
#     for hash_op in hash_ops.get_operations_for_value_type("table").values():
#
#         result = hash_op.run(table=nodes_table)
#         sampled_table = result.get_value_obj("sampled_value")
#         assert sampled_table.type_name == "table"
#         assert (
#             sampled_table.get_metadata("table")["table"]["rows"]
#             < nodes_table.get_metadata("table")["table"]["rows"]
#         )
