# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
# import os
#
# from kiara.context import Kiara
# from kiara.interfaces.python_api.operation import KiaraOperation
# from kiara.models.filesystem import FileBundle, FileModel
#
# ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# DATA_FOLDER = os.path.join(ROOT_DIR, "examples", "data")
#
#
# def test_import_file_operation(kiara: Kiara):
#
#     op = KiaraOperation(kiara=kiara, operation_name="import.file")
#
#     nodes_file = os.path.join(DATA_FOLDER, "journals", "JournalNodes1902.csv")
#
#     results = op.run(path=nodes_file)
#
#     file_value: FileModel = results.get_value_data("file")
#     assert isinstance(file_value, FileModel)
#     assert file_value.file_name == "JournalNodes1902.csv"
#
#
# def test_import_file_bundle(kiara: Kiara):
#
#     op = KiaraOperation(kiara=kiara, operation_name="import.file_bundle")
#
#     journals_folder = os.path.join(DATA_FOLDER, "journals")
#
#     results = op.run(path=journals_folder)
#
#     bundle_value: FileBundle = results.get_value_data("file_bundle")
#     assert isinstance(bundle_value, FileBundle)
#     assert "JournalNodes1902.csv" in bundle_value.included_files.keys()
