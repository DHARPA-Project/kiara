# -*- coding: utf-8 -*-
import typing

from kiara import PipelineModule
from kiara.module_config import PipelineModuleConfig
from kiara.pipeline.structure import PipelineStep


def test_workflow_desc_files(workflow_paths):

    for path in workflow_paths.values():
        c = PipelineModuleConfig.parse_file(path)
        assert isinstance(c, PipelineModuleConfig)
        assert len(c.steps) > 0
        assert c.steps[0].step_id
        assert c.steps[0].module_type


def test_workflow_obj_attributes(
    workflow_configs: typing.Mapping[str, PipelineModuleConfig]
):

    logic_1 = workflow_configs["logic_1"]

    assert len(logic_1.steps) == 1
    assert len(logic_1.input_aliases) == 0
    assert len(logic_1.output_aliases) == 0

    logic_2 = workflow_configs["logic_2"]

    assert len(logic_2.steps) == 2
    assert len(logic_2.input_aliases) == 3
    assert len(logic_2.output_aliases) == 1

    logic_3 = workflow_configs["logic_3"]

    assert len(logic_3.steps) == 3
    assert len(logic_3.input_aliases) == 0
    assert len(logic_3.output_aliases) == 0


def test_workflow_obj_creation(
    workflow_configs: typing.Mapping[str, PipelineModuleConfig]
):

    logic_1 = workflow_configs["logic_1"]
    c = PipelineModule(id="logic_1", module_config=logic_1)
    assert isinstance(c, PipelineModule)

    assert c.full_id == "logic_1"
    assert c.structure.pipeline_id == "logic_1"
    assert len(c.structure.steps) == 1
    assert "and_1" in c.structure.to_details().steps.keys()
    assert isinstance(c.structure.to_details().steps["and_1"].step, PipelineStep)
