# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# def test_workflow_desc_files(pipeline_paths):
#
#     for path in pipeline_paths.values():
#         c = PipelineConfig.parse_file(path)
#         assert isinstance(c, PipelineConfig)
#         assert len(c.steps) > 0
#         assert c.steps[0].step_id
#         assert c.steps[0].module_type

# def test_workflow_obj_attributes(pipeline_configs: typing.Mapping[str, PipelineConfig]):
#
#     logic_1 = pipeline_configs["logic_1"]
#
#     assert len(logic_1.steps) == 1
#     assert len(logic_1.input_aliases) == 0
#     assert len(logic_1.output_aliases) == 0
#
#     logic_2 = pipeline_configs["logic_2"]
#
#     assert len(logic_2.steps) == 2
#     assert len(logic_2.input_aliases) == 3
#     assert len(logic_2.output_aliases) == 1
#
#     logic_3 = pipeline_configs["logic_3"]
#
#     assert len(logic_3.steps) == 3
#     assert len(logic_3.input_aliases) == 0
#     assert len(logic_3.output_aliases) == 0
#
#
# def test_workflow_obj_creation(pipeline_configs: typing.Mapping[str, PipelineConfig]):
#
#     logic_1 = pipeline_configs["logic_1"]
#     c = PipelineModule(id="logic_1", module_config=logic_1)
#     assert isinstance(c, PipelineModule)
#
#     assert c.full_id == "logic_1"
#     assert len(c.structure.steps) == 1
#     assert "and_1" in c.structure.to_details().steps.keys()
#     assert isinstance(c.structure.to_details().steps["and_1"].step, PipelineStep)
#
#
# def test_pipeline_structure_creation(kiara: Kiara):
#
#     pipeline_file = os.path.join(PIPELINES_FOLDER, "logic", "logic_3.json")
#
#     config = PipelineConfig.create_pipeline_config(pipeline_file)
#
#     structure = PipelineStructure(config=config, kiara=kiara)
#
#     for idx, step in enumerate(structure.steps):
#         assert isinstance(step, PipelineStep)
#
#     assert idx == 2
#
#     Pipeline(structure=structure)
#
#
# def test_invalid_pipeline_creation(kiara: Kiara):
#
#     pipeline_file = os.path.join(INVALID_PIPELINES_FOLDER, "logic_4.json")
#     config = PipelineConfig.create_pipeline_config(pipeline_file)
#
#     structure = PipelineStructure(config=config, kiara=kiara)
#
#     for idx, step in enumerate(structure.steps):
#         assert isinstance(step, PipelineStep)
#     assert idx == 2
#
#     with pytest.raises(Exception) as exc_info:
#         Pipeline(structure=structure)
#     assert "a1" in str(exc_info.value)
