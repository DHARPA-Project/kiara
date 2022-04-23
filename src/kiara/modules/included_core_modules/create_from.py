# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import Any, Iterable, Mapping, Union

from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule


class CreateFromModuleConfig(KiaraModuleConfig):

    source_type: str = Field(description="The value type of the source value.")
    target_type: str = Field(description="The value type of the target.")


class CreateFromModule(KiaraModule):

    _module_type_name: str = None  # type: ignore
    _config_cls = CreateFromModuleConfig

    @classmethod
    def retrieve_supported_create_combinations(cls) -> Iterable[Mapping[str, str]]:

        result = []
        for attr in dir(cls):
            if (
                len(attr) <= 16
                or not attr.startswith("create__")
                or "__from__" not in attr
            ):
                continue

            tokens = attr.split("__")
            if len(tokens) != 4:
                continue

            source_type = tokens[3]
            target_type = tokens[1]

            data = {
                "source_type": source_type,
                "target_type": target_type,
                "func": attr,
            }
            result.append(data)
        return result

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")
        assert source_type not in ["target", "base_name"]

        schema = {
            source_type: {"type": source_type, "doc": "The value to render."},
        }

        return schema

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            self.get_config_value("target_type"): {
                "type": self.get_config_value("target_type"),
                "doc": "The result value.",
            }
        }

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        source_type = self.get_config_value("source_type")
        target_type = self.get_config_value("target_type")

        func_name = f"create__{target_type}__from__{source_type}"
        func = getattr(self, func_name)

        source_value = inputs.get_value_obj(source_type)

        result = func(source_value=source_value)
        outputs.set_value(target_type, result)
