# -*- coding: utf-8 -*-
import pytest

import typing
from kiara_modules.core.logic import AndModule

from kiara import Kiara, KiaraModule


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

    with pytest.raises(Exception) as e_info:
        l_and.run()

    assert "Inputs not valid" in str(e_info.value)

    with pytest.raises(Exception) as e_info:
        l_and.run(x=True)

    assert "Invalid input name" in str(e_info.value)

    l_xor = kiara.create_module("logic.xor")
    result = l_and.run(a=True, b=True)
    assert result.get_all_value_data() == {"y": True}

    with pytest.raises(Exception) as e_info:
        result = l_xor.run()
    assert "Inputs not valid" in str(e_info.value)

    with pytest.raises(Exception) as e_info:
        l_xor.run(x=True)

    assert "Invalid input name" in str(e_info.value)


def test_module_instance_direct(kiara: Kiara):

    and_module = AndModule(kiara=kiara)
    result = and_module.run(a=True, b=True)
    assert result.get_all_value_data() == {"y": True}


def test_module_instance_from_class(kiara: Kiara):

    and_module = AndModule.create_instance(kiara=kiara)
    result = and_module.run(a=True, b=True)
    assert result.get_all_value_data() == {"y": True}

    with pytest.raises(Exception) as e:
        AndModule.create_instance(module_type="and", kiara=kiara)

    assert "but not both" in str(e.value)


def test_module_instance_from_config_file(
    kiara: Kiara, module_config_paths: typing.Dict[str, str]
):

    for path in module_config_paths.values():
        mod = KiaraModule.create_instance(path, kiara=kiara)
        assert isinstance(mod, KiaraModule)
        assert mod.input_schemas.keys()
        assert mod.output_schemas.keys()


def test_module_instance_from_pipeline_config_files(
    kiara: Kiara, pipeline_paths: typing.Dict[str, str]
):

    for path in pipeline_paths.values():
        mod = KiaraModule.create_instance(path, kiara=kiara)
        assert isinstance(mod, KiaraModule)
        assert mod.input_schemas.keys()
        assert mod.output_schemas.keys()
