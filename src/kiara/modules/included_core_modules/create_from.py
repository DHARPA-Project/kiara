# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import inspect
from pydantic import Field
from typing import Any, Dict, Iterable, Mapping, Optional, Union

from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import Value, ValueMap, ValueMapReadOnly
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

    def create_optional_inputs(
        self, source_type: str, target_type
    ) -> Optional[Mapping[str, Mapping[str, Any]]]:
        return None

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")
        assert source_type not in ["target", "base_name"]

        target_type = self.get_config_value("target_type")
        optional = self.create_optional_inputs(
            source_type=source_type, target_type=target_type
        )

        schema = {
            source_type: {"type": source_type, "doc": "The type of the source value."},
        }
        if optional:
            for field, field_schema in optional.items():
                field_schema = dict(field_schema)
                if field in schema.keys():
                    raise Exception(
                        f"Can't create inputs schema for '{self.module_type_name}': duplicate field '{field}'."
                    )
                if field == source_type:
                    raise Exception(
                        f"Can't create inputs schema for '{self.module_type_name}': invalid field name '{field}'."
                    )

                optional = field_schema.get("optional", True)
                if not optional:
                    raise Exception(
                        f"Can't create inputs schema for '{self.module_type_name}': non-optional field '{field}' specified."
                    )
                field_schema["optional"] = True
                schema[field] = field_schema
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

        signature = inspect.signature(func)
        if "optional" in signature.parameters:
            optional: Dict[str, Value] = {}
            op_schemas = {}
            for field, schema in self.inputs_schema.items():
                if field == source_type:
                    continue
                optional[field] = inputs.get_value_obj(field)
                op_schemas[field] = schema
            result = func(
                source_value=source_value,
                optional=ValueMapReadOnly(
                    value_items=optional, values_schema=op_schemas
                ),
            )
        else:
            result = func(source_value=source_value)
        outputs.set_value(target_type, result)
