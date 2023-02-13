# -*- coding: utf-8 -*-
from kiara.api import Kiara

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_operation_type_list(kiara: Kiara):

    assert "pretty_print" in kiara.operation_registry.operation_types.keys()

    op_xor = kiara.operation_registry.get_operation("logic.xor")
    assert op_xor.module.is_pipeline()
    assert op_xor.operation_id == "logic.xor"
