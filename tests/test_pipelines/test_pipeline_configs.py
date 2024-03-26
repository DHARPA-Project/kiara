# -*- coding: utf-8 -*-
from kiara.interfaces.python_api.base_api import BaseAPI


def test_pipeline_default_config_simple(api: BaseAPI):

    pipeline_config = """
pipeline_name: test_pipeline
steps:
  - step_id: step_1
    module_type: logic.and
  - step_id: step_2
    module_type: logic.and
     """

    op = api.get_operation(pipeline_config)
    assert op is not None
    inputs_schema = op.inputs_schema
    outputs_schema = op.outputs_schema
    assert len(inputs_schema) == 4
    assert len(outputs_schema) == 2

    assert inputs_schema["step_1__a"].type == "boolean"
    assert outputs_schema["step_1__y"].type == "boolean"


def test_pipeline_config_aliases(api: BaseAPI):

    pipeline_config = """
pipeline_name: test_pipeline
steps:
  - step_id: step_1
    module_type: logic.and
  - step_id: step_2
    module_type: logic.and
input_aliases:
  step_1.a: a
  step_1.b: b
  step_2.a: c
  step_2.b: d
     """

    op = api.get_operation(pipeline_config)
    assert op is not None
    inputs_schema = op.inputs_schema
    outputs_schema = op.outputs_schema
    assert len(inputs_schema) == 4
    assert len(outputs_schema) == 2

    assert inputs_schema["a"].type == "boolean"
    assert inputs_schema["b"].type == "boolean"
    assert inputs_schema["c"].type == "boolean"
    assert inputs_schema["d"].type == "boolean"


def test_pipeline_config_aliases_2(api: BaseAPI):

    pipeline_config = """
pipeline_name: test_pipeline
steps:
  - step_id: step_1
    module_type: logic.and
  - step_id: step_2
    module_type: logic.and
input_aliases:
  step_1.a: a
  step_1.b: b
  step_2.a: a
  step_2.b: b
     """

    op = api.get_operation(pipeline_config)
    assert op is not None
    inputs_schema = op.inputs_schema
    outputs_schema = op.outputs_schema
    assert len(inputs_schema) == 2
    assert len(outputs_schema) == 2

    assert inputs_schema["a"].type == "boolean"
    assert inputs_schema["b"].type == "boolean"


def test_pipeline_module_config(api: BaseAPI):

    pipeline_config = """
pipeline_name: test_pipeline
steps:
  - step_id: step_1
    module_type: logic.and
    module_config:
      delay: 0.1
  - step_id: step_2
    module_type: logic.and
    module_config:
      delay: 0.2
input_aliases:
  step_1.a: a
  step_1.b: b
  step_2.a: a
  step_2.b: b
     """

    api.get_operation(pipeline_config)
