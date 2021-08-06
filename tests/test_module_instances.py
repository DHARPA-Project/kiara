# -*- coding: utf-8 -*-
import pytest

from kiara import Kiara


def test_module_instance_creation(kiara: Kiara):

    l_and = kiara.create_module("logic.and")
    assert l_and.config.dict() == {"constants": {}, "defaults": {}, "delay": 0}

    l_xor = kiara.create_module("logic.xor")
    assert "input_aliases" in l_xor.config.dict().keys()
    assert "output_aliases" in l_xor.config.dict().keys()

    with pytest.raises(Exception) as e_info:
        kiara.create_module("logic.and", module_config={"xxx": "fff"})

    assert "xxx" in str(e_info.value)
    assert "extra fields" in str(e_info.value)

    with pytest.raises(Exception) as e_info:
        kiara.create_module("logic.xor", module_config={"xxx": "fff"})

    assert "Can't dynamically create PipelineModuleClass" in str(e_info.value)


def test_module_instance_run(kiara: Kiara):

    l_and = kiara.create_module("logic.and")
    result = l_and.run(a=True, b=True)
    assert result.get_all_value_data() == {"y": True}

    # with pytest.raises(Exception) as e_info:
    result = l_and.run()
    assert result.get_all_value_data() == {"y": None}

    with pytest.raises(Exception) as e_info:
        l_and.run(x=True)

    assert "Invalid input name" in str(e_info.value)

    l_xor = kiara.create_module("logic.xor")
    result = l_and.run(a=True, b=True)
    assert result.get_all_value_data() == {"y": True}

    result = l_xor.run()
    assert result.get_all_value_data() == {"y": None}

    with pytest.raises(Exception) as e_info:
        l_xor.run(x=True)

    assert "Invalid input name" in str(e_info.value)
