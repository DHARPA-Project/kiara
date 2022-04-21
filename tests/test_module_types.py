# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# def test_module_types_exist(kiara: Kiara):
#
#     assert "logic.and" in kiara.module_mgmt.module_type_names
#     assert "logic.xor" in kiara.module_mgmt.module_type_names
#
#     assert "logic.and" not in kiara.module_mgmt.available_pipeline_module_types
#     assert "logic.and" in kiara.module_mgmt.available_non_pipeline_module_types
#
#     assert "logic.xor" in kiara.module_mgmt.available_pipeline_module_types
#     assert "logic.xor" not in kiara.module_mgmt.available_non_pipeline_module_types
#
#
# def test_module_type_metadata(kiara: Kiara):
#
#     l_and = kiara.module_mgmt.get_module_class("logic.and")
#     assert not l_and.is_pipeline()
#
#     l_xor = kiara.module_mgmt.get_module_class("logic.xor")
#     assert l_xor.is_pipeline()
#
#     assert hasattr(l_and, "_module_type_name")
#     assert hasattr(l_and, "_module_type_id")
#
#     assert hasattr(l_xor, "_module_type_name")
#     assert hasattr(l_xor, "_module_type_id")
#
#     assert (
#         l_and.get_type_metadata().documentation.full_doc
#         == l_and.get_type_metadata().documentation.description
#     )
#     assert (
#         l_xor.get_type_metadata().documentation.full_doc
#         == l_xor.get_type_metadata().documentation.description
#     )
#
#     assert l_and.get_type_metadata().context.labels["package"] == "kiara_modules.core"
#     assert l_xor.get_type_metadata().context.labels["package"] == "kiara_modules.core"
