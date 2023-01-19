# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os

# from kiara.context import Kiara
# from kiara.interfaces.python_api.operation import KiaraOperation
# from kiara.models.values.value import Value

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_FOLDER = os.path.join(ROOT_DIR, "examples", "data")


# def test_save_file_data(kiara: Kiara):
#
#     op = KiaraOperation(kiara=kiara, operation_name="import.file")
#
#     nodes_file = os.path.join(DATA_FOLDER, "journals", "JournalNodes1902.csv")
#
#     results = op.run(path=nodes_file)
#
#     file_value: Value = results.get_value_obj("file")
#
#     persisted_value = kiara.data_registry.store_value(value=file_value)
#
#     assert persisted_value is not None
#
#     loaded = kiara.data_registry.get_value(file_value.value_id)
#     assert loaded.value_id == file_value.value_id
#     assert loaded.data == file_value.data
#
#
# def test_save_file_bundle(kiara: Kiara):
#
#     op = KiaraOperation(kiara=kiara, operation_name="import.file_bundle")
#
#     journals_folder = os.path.join(DATA_FOLDER, "journals")
#
#     results = op.run(path=journals_folder)
#
#     bundle_value: Value = results.get_value_obj("file_bundle")
#
#     persisted_value = kiara.data_registry.store_value(value=bundle_value)
#
#     assert persisted_value is not None
#
#     loaded = kiara.data_registry.get_value(bundle_value.value_id)
#     assert loaded.value_id == bundle_value.value_id
#     assert loaded.data == bundle_value.data
