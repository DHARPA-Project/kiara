# -*- coding: utf-8 -*-
import os
import typing

from kiara import Kiara, PipelineModule
from kiara.pipeline.config import PipelineModuleConfig
from kiara.pipeline.structure import PipelineStep, PipelineStructure


def test_workflow_desc_files(pipeline_paths):

    for path in pipeline_paths.values():
        c = PipelineModuleConfig.parse_file(path)
        assert isinstance(c, PipelineModuleConfig)
        assert len(c.steps) > 0
        assert c.steps[0].step_id
        assert c.steps[0].module_type


def test_workflow_obj_attributes(
    pipeline_configs: typing.Mapping[str, PipelineModuleConfig]
):

    logic_1 = pipeline_configs["logic_1"]

    assert len(logic_1.steps) == 1
    assert len(logic_1.input_aliases) == 0
    assert len(logic_1.output_aliases) == 0

    logic_2 = pipeline_configs["logic_2"]

    assert len(logic_2.steps) == 2
    assert len(logic_2.input_aliases) == 3
    assert len(logic_2.output_aliases) == 1

    logic_3 = pipeline_configs["logic_3"]

    assert len(logic_3.steps) == 3
    assert len(logic_3.input_aliases) == 0
    assert len(logic_3.output_aliases) == 0


def test_workflow_obj_creation(
    pipeline_configs: typing.Mapping[str, PipelineModuleConfig]
):

    logic_1 = pipeline_configs["logic_1"]
    c = PipelineModule(id="logic_1", module_config=logic_1)
    assert isinstance(c, PipelineModule)

    assert c.full_id == "logic_1"
    assert c.structure.pipeline_id == "logic_1"
    assert len(c.structure.steps) == 1
    assert "and_1" in c.structure.to_details().steps.keys()
    assert isinstance(c.structure.to_details().steps["and_1"].step, PipelineStep)


def test_pipeline_structure_creation(kiara: Kiara):

    pipeline_file = os.path.join(
        os.path.dirname(__file__), "resources", "pipelines", "logic", "logic_3.json"
    )

    config = PipelineModuleConfig.from_file(pipeline_file)
    structure = PipelineStructure(parent_id="_", config=config, kiara=kiara)

    for idx, step in enumerate(structure.steps):
        assert isinstance(step, PipelineStep)

    assert idx == 2
