# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.operations import OperationType
from kiara.data.values import ValueSchema
from kiara.module_config import KiaraModuleConfig

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


class CalculateValueHashesConfig(KiaraModuleConfig):

    value_type: str = Field(
        description="The type of the value to calculate the hash for."
    )
    hash_type: str = Field(description="The hash type.")


class CalculateValueHashModule(KiaraModule):
    """Calculate the hash of a value."""

    _module_type_name = "value.hash"
    _config_cls = CalculateValueHashesConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "value_item": {
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

        value: Value = inputs.get_value_obj("value_item")

        value_hash = value.calculate_hash(hash_type=self.get_config_value("hash_type"))
        outputs.set_value("hash", value_hash)


class CalculateHashOperationType(OperationType):
    """Calculate a hash for a dataset."""

    @classmethod
    def retrieve_operation_configs(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]:

        result: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        for type_name, type_cls in kiara.type_mgmt.value_types.items():

            hash_types = type_cls.get_supported_hash_types()

            for hash_type in hash_types:

                result.setdefault(type_name, {}).setdefault("calculate_hash", {})[
                    hash_type
                ] = {
                    "module_type": "value.hash",
                    "module_config": {"value_type": type_name, "hash_type": hash_type},
                    "input_name": "value_item",
                }

        return result
