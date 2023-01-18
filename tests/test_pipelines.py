# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


#
# def test_pipeline_default_controller_invalid_inputs(kiara: Kiara):
#
#     pipeline = kiara.create_pipeline("logic.nand")
#
#     with pytest.raises(Exception) as e:
#         pipeline.inputs.set_values(logic_nand__a1=True, logic_nand__b=True)
#
#     assert "logic_nand__a1" in str(e.value)
#     assert "logic_nand__b" in str(e.value)
#
#
# def test_pipeline_default_controller_invalid_outputs(kiara: Kiara):
#
#     pipeline = kiara.create_pipeline("logic.nand")
#
#     pipeline.inputs.set_values(logic_nand__a=True, logic_nand__b=True)
#
#     with pytest.raises(KeyError) as e:
#         pipeline.outputs.get_value_data("logic_nand__y1")
#
#     assert "logic_nand__y1" in str(e.value)
#     assert "logic_nand__y" in str(e.value)
#
#
# # TODO: more complex pipelines
#
#
# def test_pipeline_default_controller_synchronous_processing(kiara: Kiara):
#
#     processor = ModuleProcessor.from_config(
#         config={"module_processor_type": "synchronous"}
#     )
#     controller = BatchController(processor=processor)
#     pipeline = kiara.create_pipeline("logic.nand", controller=controller)
#
#     pipeline.inputs.set_values(logic_nand__a=True, logic_nand__b=True)
#
#     result = pipeline.outputs.get_all_value_data()
#     assert result == {"logic_nand__y": False}
#
#
# def test_pipeline_default_controller_threaded_processing(kiara: Kiara):
#
#     processor = ModuleProcessor.from_config(
#         config={"module_processor_type": "multi-threaded"}
#     )
#     controller = BatchController(processor=processor)
#     pipeline = kiara.create_pipeline("logic.nand", controller=controller)
#
#     pipeline.inputs.set_values(logic_nand__a=True, logic_nand__b=True)
#
#     result = pipeline.outputs.get_all_value_data()
#     assert result == {"logic_nand__y": False}
