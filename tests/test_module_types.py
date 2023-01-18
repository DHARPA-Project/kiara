# -*- coding: utf-8 -*-
from kiara import Kiara

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_module_types_exist(kiara: Kiara):

    assert "logic.and" in kiara.module_registry.get_module_type_names()
    assert "logic.xor" not in kiara.module_registry.get_module_type_names()


def test_module_type_metadata(kiara: Kiara):

    l_and = kiara.module_registry.get_module_class("logic.and")
    assert not l_and.is_pipeline()

    pipeline = kiara.module_registry.get_module_class("pipeline")
    assert pipeline.is_pipeline()

    assert hasattr(l_and, "_module_type_name")
    assert hasattr(pipeline, "_module_type_name")
