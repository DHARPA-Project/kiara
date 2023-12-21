# -*- coding: utf-8 -*-

from typing import Any, ClassVar, Dict, Mapping

from boltons.strutils import slugify
from pydantic import BaseModel, Field

from kiara.api import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema
from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.models.module.pipeline import PipelineConfig
from kiara.modules import ModuleCharacteristics


class MockOutput(BaseModel):
    field_schema: Dict[str, Any] = Field(description="The schema of the output.")
    data: Any = Field(description="The data of the output.", default="mock result data")


def default_mock_output() -> Dict[str, MockOutput]:

    schema = {
        "type": "any",
        "doc": "A result",
        "optional": False,
    }
    return {"result": MockOutput(field_schema=schema, data="mock result data")}


class MockModuleConfig(KiaraModuleConfig):

    _kiara_model_id: ClassVar = "instance.module_config.mock"

    @classmethod
    def create_pipeline_config(
        cls, title: str, description: str, author: str, *steps: "MockModuleConfig"
    ) -> PipelineConfig:

        data: Dict[str, Any] = {
            "pipeline_name": slugify(title),
            "doc": description,
            "context": {"authors": [author]},
            "steps": [],
        }
        for step in steps:
            step_data = {
                "step_id": slugify(step.title),
                "module_type": "dummy",
                "module_config": {
                    "title": step.title,
                    "inputs_schema": step.inputs_schema,
                    "outputs": step.outputs,
                    "desc": step.desc,
                },
            }
            data["steps"].append(step_data)

        pipeline_config = PipelineConfig.from_config(data)
        return pipeline_config

    inputs_schema: Dict[str, Dict[str, Any]] = Field(
        description="The input fields and their types.",
    )

    outputs: Dict[str, MockOutput] = Field(
        description="The outputs fields of the operation, along with their types and mock data.",
        default_factory=default_mock_output,
    )

    title: str = Field(
        description="The title of this operation.", default="mock_operation"
    )
    desc: str = Field(
        description="A description of what this step does.",
        default=DEFAULT_NO_DESC_VALUE,
    )


class MockKiaraModule(KiaraModule):

    _module_type_name = "mock"
    _config_cls = MockModuleConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        result = {}
        v: Mapping[str, Any]
        for k, v in self.get_config_value("inputs_schema").items():
            data = {
                "type": v["type"],
                "doc": v.get("doc", "-- n/a --"),
                "optional": v.get("optional", True),
            }
            result[k] = data

        return result

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        result = {}
        field_name: str
        field_output: MockOutput
        for field_name, field_output in self.get_config_value("outputs").items():
            field_schema = field_output.field_schema
            if field_schema:
                data = {
                    "type": field_schema["type"],
                    "doc": field_schema.get("doc", DEFAULT_NO_DESC_VALUE),
                    "optional": field_schema.get("optional", False),
                }
            else:
                data = {
                    "type": "any",
                    "doc": DEFAULT_NO_DESC_VALUE,
                    "optional": False,
                }
            result[field_name] = data

        return result

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:

        return ModuleCharacteristics(
            is_idempotent=True, is_internal=True, unique_result_values=True
        )

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        # config = self.get_config_value("desc")

        mock_outputs = self.get_config_value("outputs")
        field_name: str
        field_output: MockOutput
        for field_name, field_output in mock_outputs.items():

            outputs.set_value(field_name, field_output.data)
