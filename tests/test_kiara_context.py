# -*- coding: utf-8 -*-
from kiara import Kiara


def test_multiple_kiara_instances():

    kiara = Kiara()
    kiara_2 = Kiara()

    assert kiara.available_operation_ids == kiara_2.available_operation_ids
    assert kiara.available_module_types == kiara_2.available_module_types
