# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import Any, Iterable, Mapping, Tuple, Union

from pydantic import Field

from kiara.exceptions import KiaraProcessingException
from kiara.models.data_types import KiaraDict
from kiara.models.module import KiaraModuleConfig
from kiara.models.rendering import RenderValueResult
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import (
    DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS,
    KiaraModule,
    ModuleCharacteristics,
)
from kiara.utils import log_message


class RenderValueModuleConfig(KiaraModuleConfig):

    # render_scene_type: str = Field(
    #     description="The id of the model that describes (and handles) the actual rendering."
    # )
    source_type: str = Field(description="The (kiara) data type to be rendered.")
    target_type: str = Field(
        description="The (kiara) data type of210 the rendered result."
    )

    # @validator("render_scene_type")
    # def validate_render_scene(cls, value: Any):
    #
    #     registry = ModelRegistry.instance()
    #
    #     if value not in registry.all_models.item_infos.keys():
    #         raise ValueError(
    #             f"Invalid model type '{value}'. Value model ids: {', '.join(registry.all_models.item_infos.keys())}."
    #         )
    #
    #     return value


class RenderValueModule(KiaraModule):
    @classmethod
    def retrieve_supported_render_combinations(cls) -> Iterable[Tuple[str, str]]:

        result = []
        for attr in dir(cls):
            if (
                len(attr) <= 16
                or not attr.startswith("render__")
                or "__as__" not in attr
            ):
                continue

            attr = attr[8:]
            end_start_type = attr.find("__as__")
            source_type = attr[0:end_start_type]
            target_type = attr[end_start_type + 6 :]
            result.append((source_type, target_type))
        return result

    _config_cls = RenderValueModuleConfig
    _module_type_name: str = None  # type: ignore

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        # instruction = self.get_config_value("render_scene_type")
        # model_registry = ModelRegistry.instance()
        # instr_model_cls: Type[RenderScene] = model_registry.get_model_cls(instruction, required_subclass=RenderScene)  # type: ignore

        # data_type_name = instr_model_cls.retrieve_source_type()
        # assert data_type_name

        source_type = self.get_config_value("source_type")
        optional = source_type == "none"
        inputs = {
            "value": {
                "type": source_type,
                "doc": f"A value of type '{source_type}'",
                "optional": optional,
            },
            "render_config": {
                "type": "dict",
                "doc": "Instructions/config on how (or what) to render the provided value.",
                "default": {},
            },
        }
        return inputs

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        outputs = {
            "render_value_result": {
                "type": "render_value_result",
                "doc": "The rendered value, incl. some metadata.",
            },
        }

        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        source_type = self.get_config_value("source_type")
        target_type = self.get_config_value("target_type")

        value: Value = inputs.get_value_obj("value")

        render_scene: KiaraDict = inputs.get_value_data("render_config")
        if render_scene:
            rc = render_scene.dict_data
        else:
            rc = {}

        func_name = f"render__{source_type}__as__{target_type}"

        func = getattr(self, func_name)
        result = func(value=value, render_config=rc)
        if isinstance(result, RenderValueResult):
            render_scene_result: RenderValueResult = result
        else:
            render_scene_result = RenderValueResult(
                value_id=value.value_id,
                render_config=rc,
                render_manifest=self.manifest.manifest_hash,
                rendered=result,
                related_scenes={},
            )
        render_scene_result.manifest_lookup[self.manifest.manifest_hash] = self.manifest

        outputs.set_value("render_value_result", render_scene_result)


class ValueTypeRenderModule(KiaraModule):
    """A module that uses render methods attached to DataType classes."""

    _module_type_name = "render.value"
    _config_cls = RenderValueModuleConfig

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")
        assert source_type not in ["target", "base_name"]

        schema = {
            "value": {
                "type": source_type,
                "doc": "The value to render.",
                "optional": False,
            },
            "render_config": {
                "type": "dict",
                "doc": "Instructions/config on how (or what) to render the provided value.",
                "default": {},
            },
        }

        return schema

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        outputs = {
            "render_value_result": {
                "type": "render_value_result",
                "doc": "The rendered value, incl. some metadata.",
            },
        }

        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap):

        source_value = inputs.get_value_obj("value")
        if not source_value.is_set:
            raise KiaraProcessingException(
                f"Can't render value '{source_value.value_id}': value not set."
            )

        # source_type = self.get_config_value("source_type")
        target_type = self.get_config_value("target_type")

        render_scene: KiaraDict = inputs.get_value_data("render_config")

        try:
            data_type_cls = source_value.data_type_info.data_type_class.get_class()
            data_type = data_type_cls(**source_value.value_schema.type_config)

        except Exception as e:
            source_data_type = source_value.data_type_name
            log_message("data_type.unknown", data_type=source_data_type, error=e)

            from kiara.data_types.included_core_types import AnyType

            data_type = AnyType()

        func_name = f"render_as__{target_type}"
        func = getattr(data_type, func_name)

        if render_scene:
            rc = render_scene.dict_data
        else:
            rc = {}

        result = func(
            value=source_value,
            render_config=rc,
            manifest=self.manifest,
        )

        if isinstance(result, RenderValueResult):
            render_scene_result = result
        else:
            render_scene_result = RenderValueResult(
                value_id=source_value.value_id,
                render_config=rc,
                render_manifest=self.manifest.manifest_hash,
                rendered=result,
                related_scenes={},
                manifest_lookup={self.manifest.manifest_hash: self.manifest},
            )

        outputs.set_value("render_value_result", render_scene_result)
