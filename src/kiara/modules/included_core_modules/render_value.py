# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
import pprint
from pydantic import BaseModel, Field, validator
from typing import Any, Dict, Iterable, Mapping, Tuple, Union

from kiara.models import KiaraModel
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule
from kiara.utils.output import create_table_from_model_object


class RenderValueConfig(KiaraModuleConfig):

    source_type: str = Field(description="The value type of the source value.")
    target_type: str = Field(description="The value type of the rendered value.")

    @validator("source_type")
    def validate_source_type(cls, value):
        if value == "render_config":
            raise ValueError(f"Invalid source type: {value}.")
        return value


class RenderValueModule(KiaraModule):

    _module_type_name: str = None  # type: ignore
    _config_cls = RenderValueConfig

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
            target_type = attr[end_start_type + 6 :]  # noqa
            result.append((source_type, target_type))
        return result

    # def create_persistence_config_schema(self) -> Optional[Mapping[str, Mapping[str, Any]]]:
    #     return None

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")
        assert source_type not in ["target", "base_name"]

        schema = {
            source_type: {"type": source_type, "doc": "The value to render."},
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

        value = inputs.get_value_obj(source_type)
        render_config = inputs.get_value_data("render_config")

        func_name = f"render__{source_type}__as__{target_type}"

        func = getattr(self, func_name)
        # TODO: check function signature is valid
        result = func(value=value, render_config=render_config)

        outputs.set_value("rendered_value", result)


class ValueTypeRenderModule(KiaraModule):

    _module_type_name = "value.render"
    _config_cls = RenderValueConfig

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")
        assert source_type not in ["target", "base_name"]

        schema = {
            source_type: {"type": source_type, "doc": "The value to render."},
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

        source_value = inputs.get_value_obj(source_type)
        render_config = inputs.get_value_obj("render_config")

        data_type_cls = source_value.data_type_class.get_class()
        data_type = data_type_cls(**source_value.value_schema.type_config)

        func_name = f"render_as__{target_type}"
        func = getattr(data_type, func_name)

        render_config_dict = render_config.data
        if render_config_dict is None:
            render_config_dict = {}

        result = func(value=source_value, render_config=render_config_dict)
        # TODO: check we have the correct type?
        outputs.set_value("rendered_value", result)


class RenderAnyValueModule(RenderValueModule):

    _module_type_name = "value.render.any"

    def render__any__as__string(self, value: Value, render_config: Dict[str, Any]):

        data = value.data
        if isinstance(data, KiaraModel):
            return data.json(option=orjson.OPT_INDENT_2)
        else:
            return str(data)

    def render__any__as__terminal_renderable(
        self, value: Value, render_config: Dict[str, Any]
    ):

        data = value.data

        if isinstance(data, BaseModel):
            rendered = create_table_from_model_object(
                model=data, render_config=render_config
            )
        elif isinstance(data, Iterable):
            rendered = pprint.pformat(data)
        else:
            rendered = str(data)
        return rendered
