# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.values import ValueSchema
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


class CalculateValueHashesConfig(ModuleTypeConfigSchema):

    value_type: str = Field(
        description="The type of the value to calculate the hash for."
    )
    hash_type: str = Field(description="The hash type.")


class CalculateValueHashModule(KiaraModule):
    """Calculate the hash of a value."""

    _module_type_name = "value.hash"
    _config_cls = CalculateValueHashesConfig

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        for value_type_name, value_type in kiara.type_mgmt.value_types.items():

            hash_types = value_type.get_supported_hash_types()

            for ht in hash_types:
                op_config = {
                    "module_type": cls._module_type_id,  # type: ignore
                    "module_config": {"value_type": value_type_name, "hash_type": ht},
                    "doc": f"Calculate '{ht}' hash for value type '{value_type_name}'.",
                }
                all_metadata_profiles[
                    f"calculate_hash.{ht}.for.{value_type_name}"
                ] = op_config

        return all_metadata_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        input_name = self.get_config_value("value_type")
        if input_name == "any":
            input_name = "value_item"

        return {
            input_name: {
                "type": self.get_config_value("value_type"),
                "doc": f"A value of type '{self.get_config_value('value_type')}'.",
            }
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"hash": {"type": "string", "doc": "The hash string."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        input_name = self.get_config_value("value_type")
        if input_name == "any":
            input_name = "value_item"
        value: Value = inputs.get_value_obj(input_name)

        value_hash = value.get_hash(hash_type=self.get_config_value("hash_type"))
        outputs.set_value("hash", value_hash.hash)


class CalculateHashOperationType(OperationType):
    """Operation type to calculate hash(es) for data.

    This is mostly used internally, so users usually won't be exposed to operations of this type.
    """

    def is_matching_operation(self, op_config: Operation) -> bool:

        return op_config.module_cls == CalculateValueHashModule

    def get_hash_operations_for_type(
        self, value_type: str
    ) -> typing.Dict[str, Operation]:

        result = {}
        for op_config in self.operations.values():
            if op_config.module_config["value_type"] == value_type:
                result[op_config.module_config["hash_type"]] = op_config

        return result
