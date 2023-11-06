# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import Any, Iterable, Mapping, Tuple, Union

from pydantic import Field, field_validator

from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import (
    DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS,
    KiaraModule,
    ModuleCharacteristics,
)
from kiara.utils import log_message


class PrettyPrintConfig(KiaraModuleConfig):

    source_type: str = Field(description="The value type of the source value.")
    target_type: str = Field(description="The value type of the rendered value.")

    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, value):
        if value == "render_config":
            raise ValueError(f"Invalid source type: {value}.")
        return value


class PrettyPrintModule(KiaraModule):

    _module_type_name: str = None  # type: ignore
    _config_cls = PrettyPrintConfig

    @classmethod
    def retrieve_supported_render_combinations(cls) -> Iterable[Tuple[str, str]]:

        result = []
        for attr in dir(cls):
            if (
                len(attr) <= 19
                or not attr.startswith("pretty_print__")
                or "__as__" not in attr
            ):
                continue

            attr = attr[14:]
            end_start_type = attr.find("__as__")
            source_type = attr[0:end_start_type]
            target_type = attr[end_start_type + 6 :]
            result.append((source_type, target_type))
        return result

    # def create_persistence_config_schema(self) -> Optional[Mapping[str, Mapping[str, Any]]]:
    #     return None

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")
        assert source_type not in ["target", "base_name"]

        schema = {
            "value": {"type": source_type, "doc": "The value to render."},
            "render_config": {
                "type": "any",
                "doc": "Value type dependent render configuration.",
                "optional": True,
            },
        }

        return schema

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "rendered_value": {
                "type": self.get_config_value("target_type"),
                "doc": "The rendered value.",
            }
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        source_type = self.get_config_value("source_type")
        target_type = self.get_config_value("target_type")

        value = inputs.get_value_obj("value")
        render_config = inputs.get_value_data("render_config")

        func_name = f"pretty_print__{source_type}__as__{target_type}"

        func = getattr(self, func_name)
        # TODO: check function signature is valid

        if render_config is None:
            render_config = {}

        result = func(value=value, render_config=render_config)

        outputs.set_value("rendered_value", result)


class ValueTypePrettyPrintModule(KiaraModule):

    _module_type_name = "pretty_print.value"
    _config_cls = PrettyPrintConfig

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
                "optional": True,
            },
            "render_config": {
                "type": "any",
                "doc": "Value type dependent render configuration.",
                "optional": True,
            },
        }

        return schema

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "rendered_value": {
                "type": self.get_config_value("target_type"),
                "doc": "The rendered value.",
            }
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        # source_type = self.get_config_value("source_type")
        target_type = self.get_config_value("target_type")

        source_value = inputs.get_value_obj("value")
        render_config = inputs.get_value_obj("render_config")

        if not source_value.is_set:
            outputs.set_value("rendered_value", "-- none/not set --")
            return

        try:
            data_type_cls = source_value.data_type_info.data_type_class.get_class()
            data_type = data_type_cls(**source_value.value_schema.type_config)
        except Exception as e:
            source_data_type = source_value.data_type_name
            log_message("data_type.unknown", data_type=source_data_type, error=e)

            from kiara.data_types.included_core_types import AnyType

            data_type = AnyType()

        func_name = f"pretty_print_as__{target_type}"
        func = getattr(data_type, func_name)

        render_config_dict = render_config.data
        if render_config_dict is None:
            render_config_dict = {}

        result = func(value=source_value, render_config=render_config_dict)
        # TODO: check we have the correct type?
        outputs.set_value("rendered_value", result)


class PrettyPrintAnyValueModule(PrettyPrintModule):

    _module_type_name = "pretty_print.any.value"

    # def pretty_print__any__as__string(self, value: Value, render_config: Dict[str, Any]):
    #
    #     data = value.data
    #     if isinstance(data, KiaraModel):
    #         return data.model_dump_json(option=orjson.OPT_INDENT_2)
    #     else:
    #         return str(data)
