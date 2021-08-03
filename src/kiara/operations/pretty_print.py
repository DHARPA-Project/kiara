# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.module_config import ModuleTypeConfig
from kiara.operations import OperationConfig, Operations
from kiara.utils import log_message


class PrettyPrintModuleConfig(ModuleTypeConfig):

    value_type: str = Field(description="The type of the value to save.")
    target_type: str = Field(
        description="The target to print the value to.", default="string"
    )


class PrettyPrintValueModule(KiaraModule):

    _config_cls = PrettyPrintModuleConfig

    @classmethod
    def get_supported_source_types(cls) -> typing.Set[str]:
        _types = cls.retrieve_supported_source_types()
        if isinstance(_types, str):
            _types = [_types]

        return set(_types)

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_source_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        pass

    @classmethod
    def get_supported_target_types(cls) -> typing.Set[str]:
        _types = cls.retrieve_supported_target_types()
        if isinstance(_types, str):
            _types = [_types]

        return set(_types)

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_target_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        pass

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: Kiara
    ) -> typing.Mapping[
        str, typing.Union[typing.Mapping[str, typing.Any], OperationConfig]
    ]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        sup_types: typing.Iterable[str] = cls.get_supported_source_types()
        if "*" in sup_types:
            sup_types = kiara.type_mgmt.value_type_names

        for sup_type in sup_types:

            if sup_type not in kiara.type_mgmt.value_type_names:
                log_message(
                    f"Ignoring pretty print operation for source type '{sup_type}': type not available"
                )
                continue

            target_types = cls.get_supported_target_types()
            for tar_type in target_types:
                if tar_type not in kiara.type_mgmt.value_type_names:
                    log_message(
                        f"Ignoring pretty print operation for target type '{tar_type}': type not available"
                    )
                    continue

                op_config = {
                    "module_type": cls._module_type_id,  # type: ignore
                    "module_config": {"value_type": sup_type, "target_type": tar_type},
                }
                all_metadata_profiles[
                    f"{sup_type}.pretty_print.as.{tar_type}"
                ] = op_config

        return all_metadata_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "value_item": {
                "type": self.get_config_value("value_type"),
                "doc": f"A value of type '{self.get_config_value('value_type')}'.",
            },
            "print_config": {
                "type": "dict",
                "doc": "Optional print configuration.",
                "optional": True,
            },
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "printed": {
                "type": self.get_config_value("target_type"),
                "doc": f"The printed value as '{self.get_config_value('target_type')}'.",
            }
        }
        return outputs

    @abc.abstractmethod
    def pretty_print(
        self,
        value: Value,
        value_type: str,
        target_type: str,
        print_config: typing.Mapping[str, typing.Any],
    ) -> typing.Any:
        """Render the value."""

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value_obj: Value = inputs.get_value_obj("value_item")
        print_config = dict(DEFAULT_PRETTY_PRINT_CONFIG)
        config: typing.Mapping = inputs.get_value_data("print_config")
        if config:
            print_config.update(config)

        value_type: str = self.get_config_value("value_type")
        target_type: str = self.get_config_value("target_type")

        result = self.pretty_print(
            value=value_obj,
            value_type=value_type,
            target_type=target_type,
            print_config=print_config,
        )
        outputs.set_value("printed", result)


class PrettyPrintOperations(Operations):
    def is_matching_operation(self, op_config: OperationConfig) -> bool:

        return issubclass(op_config.module_cls, PrettyPrintValueModule)

    def get_pretty_print_operation(
        self, value_type: str, target_type: str
    ) -> OperationConfig:

        result = []
        for op_config in self.operation_configs.values():
            if op_config.module_config["value_type"] != value_type:
                continue
            if op_config.module_config["target_type"] != target_type:
                continue
            result.append(op_config)

        if not result:
            raise Exception(
                f"No pretty print operation for value type '{value_type}' and output '{target_type}' registered."
            )
        elif len(result) != 1:
            raise Exception(
                f"Multiple pretty print operations for value type '{value_type}' and output '{target_type}' registered."
            )

        return result[0]

    def pretty_print(
        self,
        value: Value,
        target_type: str,
        print_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> typing.Any:

        ops_config = self.get_pretty_print_operation(
            value_type=value.type_name, target_type=target_type
        )
        result = ops_config.module.run(value_item=value, print_config=print_config)
        printed = result.get_value_data("printed")

        return printed
