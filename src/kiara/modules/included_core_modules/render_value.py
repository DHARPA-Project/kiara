# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field, validator
from typing import Any, Mapping, Type, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.info import TypeInfo
from kiara.models.module import KiaraModuleConfig
from kiara.models.render_value import (
    RenderInstruction,
    RenderMetadata,
    RenderValueResult,
)
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule
from kiara.registries.models import ModelRegistry


class RenderValueModuleConfig(KiaraModuleConfig):

    render_instruction_type: str = Field(
        description="The id of the model that describes (and handles) the actual rendering."
    )
    target_type: str = Field(description="The type of the rendered result.")

    @validator("render_instruction_type")
    def validate_render_instruction(cls, value: Any):

        registry = ModelRegistry.instance()

        if value not in registry.all_models.keys():
            raise ValueError(
                f"Invalid model type '{value}'. Value model ids: {', '.join(registry.all_models.keys())}."
            )

        return value


class RenderValueModule(KiaraModule):

    _config_cls = RenderValueModuleConfig
    _module_type_name: str = "render.value"

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        instruction = self.get_config_value("render_instruction_type")
        model_registry = ModelRegistry.instance()
        instr_model_cls: Type[RenderInstruction] = model_registry.get_model_cls(instruction, required_subclass=RenderInstruction)  # type: ignore

        data_type_name = instr_model_cls.retrieve_source_type()
        assert data_type_name

        inputs = {
            data_type_name: {
                "type": data_type_name,
                "doc": f"A value of type '{data_type_name}'",
                "optional": True,
            },
            "render_instruction": {
                "type": "render_instruction",
                "doc": "Instructions/config on how (or what) to render the provided value.",
                "optional": False,
                "default": {"number_of_rows": 20, "row_offset": 0, "columns": None},
            },
        }
        return inputs

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        result_model_type: str = self.get_config_value("target_type")

        outputs = {
            result_model_type: {"type": result_model_type, "doc": "The rendered data."},
            "render_metadata": {
                "type": "render_metadata",
            },
        }

        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        instruction_type = self.get_config_value("render_instruction_type")
        model_registry = ModelRegistry.instance()
        instr_info: TypeInfo = model_registry.all_models.get(instruction_type)  # type: ignore
        instr_model: Type[RenderInstruction] = instr_info.python_class.get_class()  # type: ignore

        data_type_name = instr_model.retrieve_source_type()

        render_instruction: RenderInstruction = inputs.get_value_data(
            "render_instruction"
        )
        if not issubclass(render_instruction.__class__, instr_model):
            raise KiaraProcessingException(
                f"Invalid type for 'render_instruction': must be a subclass of '{instr_model.__name__}'."
            )

        result_model_type: str = self.get_config_value("target_type")

        value: Value = inputs.get_value_obj(data_type_name)

        func_name = f"render_as__{result_model_type}"

        func = getattr(render_instruction, func_name)
        rendered: Union[RenderValueResult, Any] = func(value=value)
        try:
            rendered_value = rendered.rendered
            metadata = rendered.metadata
        except Exception:
            rendered_value = rendered
            metadata = None

        if not metadata:
            metadata = RenderMetadata()

        outputs.set_values(
            **{result_model_type: rendered_value, "render_metadata": metadata}
        )
