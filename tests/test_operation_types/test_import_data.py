# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
from kiara_modules.core.metadata_schemas import KiaraFile, KiaraFileBundle

from kiara import Kiara
from kiara.operations.data_import import DataImportModule, ImportDataOperationType

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_FOLDER = os.path.join(ROOT_DIR, "examples", "data")


def test_import_file_data(kiara: Kiara):

    op_type: ImportDataOperationType = kiara.operation_mgmt.operation_types["import"]

    nodes_file = os.path.join(DATA_FOLDER, "journals", "JournalNodes1902.csv")

    import_ops = op_type.get_import_operations_for_target_type("file")

    from_local_file_path_op: DataImportModule = import_ops["string"]["file_path"]

    result = from_local_file_path_op.run(file_path=nodes_file)
    file_value: KiaraFile = result.get_value_data("file")
    assert isinstance(file_value, KiaraFile)
    assert file_value.file_name == "JournalNodes1902.csv"


def test_import_file_bundle(kiara: Kiara):

    op_type: ImportDataOperationType = kiara.operation_mgmt.operation_types["import"]

    journals_folder = os.path.join(DATA_FOLDER, "journals")

    import_ops = op_type.get_import_operations_for_target_type("file_bundle")

    from_local_file_path_op: DataImportModule = import_ops["string"]["folder_path"]

    result = from_local_file_path_op.run(folder_path=journals_folder)
    bundle_value: KiaraFileBundle = result.get_value_data("file_bundle")
    assert isinstance(bundle_value, KiaraFileBundle)
    assert "JournalNodes1902.csv" in bundle_value.included_files.keys()
