# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os

from kiara import Kiara
from kiara.data import Value
from kiara.operations.data_import import DataImportModule, ImportDataOperationType

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_FOLDER = os.path.join(ROOT_DIR, "examples", "data")


def test_save_file_data(kiara: Kiara):

    op_type: ImportDataOperationType = kiara.operation_mgmt.operation_types["import"]

    nodes_file = os.path.join(DATA_FOLDER, "journals", "JournalNodes1902.csv")

    import_ops = op_type.get_import_operations_for_target_type("file")

    from_local_file_path_op: DataImportModule = import_ops["string"]["file_path"]

    result = from_local_file_path_op.run(file_path=nodes_file)
    file_value: Value = result.get_value_obj("file")

    saved = file_value.save("test_value_1")

    loaded_value_obj = kiara.data_store.get_value_obj(saved.id)

    assert saved.id == loaded_value_obj.id
    assert saved.get_value_data() == loaded_value_obj.get_value_data()


def test_import_file_bundle(kiara: Kiara):

    op_type: ImportDataOperationType = kiara.operation_mgmt.operation_types["import"]

    journals_folder = os.path.join(DATA_FOLDER, "journals")

    import_ops = op_type.get_import_operations_for_target_type("file_bundle")

    from_local_file_path_op: DataImportModule = import_ops["string"]["folder_path"]

    result = from_local_file_path_op.run(folder_path=journals_folder)
    bundle_value: Value = result.get_value_obj("file_bundle")

    saved = bundle_value.save("test_value_1")

    loaded_value_obj = kiara.data_store.get_value_obj(saved.id)

    assert saved.id == loaded_value_obj.id
    assert saved.get_value_data() == loaded_value_obj.get_value_data()
