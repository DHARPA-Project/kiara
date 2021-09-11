# -*- coding: utf-8 -*-
import abc
import logging
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.metadata.data import LoadConfig
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message

log = logging.getLogger("kiara")


class SaveValueModuleConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The type of the value to save.")


class SaveValueTypeModule(KiaraModule):
    """Save a specific value type.

    This is used internally.
    """

    _config_cls = SaveValueModuleConfig

    @classmethod
    def get_supported_value_types(cls) -> typing.Set[str]:
        _types = cls.retrieve_supported_types()
        if isinstance(_types, str):
            _types = [_types]

        return set(_types)

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        pass

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: Kiara
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        for sup_type in cls.get_supported_value_types():

            if sup_type not in kiara.type_mgmt.value_type_names:
                log_message(
                    f"Ignoring save operation for type '{sup_type}': type not available"
                )

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": {"value_type": sup_type},
                "doc": f"Save value of type '{sup_type}'.",
            }
            all_metadata_profiles[f"{sup_type}.save"] = op_config

        return all_metadata_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "value_id": {
                "type": "string",
                "doc": "The id to use when saving the value.",
            },
            "value_item": {
                "type": self.get_config_value("value_type"),
                "doc": f"A value of type '{self.get_config_value('value_type')}'.",
            },
            "base_path": {
                "type": "string",
                "doc": "The base path to save the value to.",
            },
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "load_config": {
                "type": "load_config",
                "doc": "The configuration to use with kiara to load the saved value.",
            }
        }

        return outputs

    @abc.abstractmethod
    def save_value(self, value: Value, base_path: str) -> typing.Dict[str, typing.Any]:
        """Save the value, and return the load config needed to load it again."""

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value_id: str = inputs.get_value_data("value_id")
        if not value_id:
            raise KiaraProcessingException("No value id provided.")
        value_obj: Value = inputs.get_value_obj("value_item")
        base_path: str = inputs.get_value_data("base_path")

        load_config = self.save_value(value=value_obj, base_path=base_path)
        load_config["value_id"] = value_id

        lc = LoadConfig(**load_config)

        if lc.base_path_input_name and lc.base_path_input_name not in lc.inputs.keys():
            raise KiaraProcessingException(
                f"Invalid load config: base path '{lc.base_path_input_name}' not part of inputs."
            )

        outputs.set_values(load_config=lc)


class SaveOperationType(OperationType):
    """Save a value into a data store."""

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, SaveValueTypeModule)

    def get_save_operation_for_type(self, value_type: str) -> Operation:

        result = []

        for op_config in self.operation_configs.values():
            if op_config.module_config["value_type"] == value_type:
                result.append(op_config)

        if not result:
            raise Exception(f"No save operation for type '{value_type}' registered.")
        elif len(result) != 1:
            raise Exception(
                f"Multiple save operations for type '{value_type}' registered."
            )

        return result[0]
