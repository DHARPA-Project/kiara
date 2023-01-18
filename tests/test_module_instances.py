# -*- coding: utf-8 -*-
import pytest

from kiara_plugin.core_types.modules.boolean import AndModule

from kiara import Kiara
from kiara.exceptions import (
    InvalidManifestException,
    InvalidValuesException,
    KiaraException,
)
from kiara.models.module.manifest import Manifest

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_module_instance_creation(kiara: Kiara):

    l_and = kiara.module_registry.create_module("logic.and")
    assert l_and.config.dict() == {"constants": {}, "defaults": {}, "delay": 0}

    with pytest.raises(InvalidManifestException):
        kiara.module_registry.create_module("logic.xor")

    l_and = kiara.module_registry.create_module("logic.and")
    assert "delay" in l_and.config.dict().keys()

    with pytest.raises(InvalidManifestException) as e_info:
        manifest = Manifest(module_type="logic.and", module_config={"xxx": "fff"})
        kiara.module_registry.create_module(manifest)

    msg = KiaraException.get_root_details(e_info.value)
    assert "xxx" in msg
    assert "extra fields" in msg


def test_module_instance_run(kiara: Kiara):

    l_and = kiara.module_registry.create_module("logic.and")

    result = l_and.run(kiara, a=True, b=True)
    assert result.get_all_value_data() == {"y": True}

    with pytest.raises(InvalidValuesException) as e_info:
        l_and.run(kiara=kiara)

    assert "Invalid inputs for module" in str(e_info.value)

    with pytest.raises(InvalidValuesException) as e_info:
        l_and.run(kiara=kiara, x=True)

    assert "Invalid inputs for module" in str(e_info.value)


def test_module_instance_direct(kiara: Kiara):

    and_module = AndModule()
    result = and_module.run(kiara=kiara, a=True, b=True)
    assert result.get_all_value_data() == {"y": True}
