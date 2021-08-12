# -*- coding: utf-8 -*-
import pytest

from kiara import Kiara


def test_pipeline_default_controller(kiara: Kiara):

    pipeline = kiara.create_pipeline("logic.nand")

    pipeline.inputs.set_values(logic_nand__a=True, logic_nand__b=True)

    result = pipeline.outputs.get_all_value_data()
    assert result == {"logic_nand__y": False}


def test_pipeline_default_controller_invalid_inputs(kiara: Kiara):

    pipeline = kiara.create_pipeline("logic.nand")

    with pytest.raises(Exception) as e:
        pipeline.inputs.set_values(logic_nand__a1=True, logic_nand__b=True)

    assert "logic_nand__a1" in str(e.value)
    assert "logic_nand__b" in str(e.value)


def test_pipeline_default_controller_invalid_outputs(kiara: Kiara):

    pipeline = kiara.create_pipeline("logic.nand")

    pipeline.inputs.set_values(logic_nand__a=True, logic_nand__b=True)

    with pytest.raises(KeyError) as e:
        pipeline.outputs.get_value_data("logic_nand__y1")

    assert "logic_nand__y1" in str(e.value)
    assert "logic_nand__y" in str(e.value)
