# -*- coding: utf-8 -*-

from kiara import Kiara
from kiara.info.pipelines import PipelineModuleInfo
from kiara.utils.output import rich_print


def test_pipeline_info(kiara: Kiara):

    pipeline_info = PipelineModuleInfo.from_type_name("logic.xor", kiara=kiara)
    # make sure conversion to renderables works
    rich_print(pipeline_info)

    # TODO: check content


def test_pipeline_current_state(kiara: Kiara):

    pipeline = kiara.create_pipeline("logic.nand")
    state = pipeline.get_current_state()

    rich_print(state)

    state_dict = state.dict()

    assert "structure" in state_dict.keys()

    assert not state_dict["pipeline_inputs"]["values"]["logic_nand__a"]["is_set"]
    assert not state_dict["pipeline_outputs"]["values"]["logic_nand__y"]["is_set"]

    pipeline.inputs.set_values(logic_nand__a=True, logic_nand__b=True)
    state = pipeline.get_current_state()
    state_dict = state.dict()

    assert state_dict["pipeline_inputs"]["values"]["logic_nand__a"]["is_set"]
    assert state_dict["pipeline_outputs"]["values"]["logic_nand__y"]["is_set"]
