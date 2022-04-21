# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# def test_multiple_kiara_instances():
#
#     kiara = Kiara()
#     kiara_2 = Kiara()
#
#     assert kiara.available_operation_ids == kiara_2.available_operation_ids
#     assert kiara.available_module_types == kiara_2.available_module_types
#
#
# def test_multiple_kiara_instances_threaded():
#     def create_kiara_context(ops: typing.List[str]):
#         kiara = Kiara()
#         ops.extend(kiara.available_operation_ids)
#
#     ops_1 = []
#     ops_2 = []
#
#     thread_1 = threading.Thread(target=create_kiara_context, args=(ops_1,))
#     thread_1.start()
#
#     thread_2 = threading.Thread(target=create_kiara_context, args=(ops_2,))
#     thread_2.start()
#
#     thread_1.join()
#     thread_2.join()
#
#     assert ops_1
#     assert ops_1 == ops_2
