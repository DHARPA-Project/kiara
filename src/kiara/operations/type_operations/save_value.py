# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.metadata.core_models import LoadConfig
from kiara.module_config import KiaraModuleConfig
from kiara.operations.type_operations import TypeOperationConfig


class SaveValueModuleConfig(KiaraModuleConfig):

    value_type: str = Field(description="The type of the value to save.")


class SaveValueTypeModule(KiaraModule):

    _config_cls = SaveValueModuleConfig

    @classmethod
    def get_supported_value_types(cls) -> typing.Set[str]:
        _types = cls._get_supported_types()
        if isinstance(_types, str):
            _types = [_types]

        return set(_types)

    @classmethod
    @abc.abstractmethod
    def _get_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        pass

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "value_id": {"type": "string", "doc": "The id of the saved value."},
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
            },
            "value_id": {"type": "string", "doc": "The id of the saved value."},
        }

        return outputs

    @abc.abstractmethod
    def save_value(
        self, value: Value, value_id: str, base_path: str
    ) -> typing.Dict[str, typing.Any]:
        """Save the value, and return the load config needed to load it again."""

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value_id: str = inputs.get_value_data("value_id")
        value_obj: Value = inputs.get_value_obj("value_item")
        base_path: str = inputs.get_value_data("base_path")

        load_config = self.save_value(
            value=value_obj, value_id=value_id, base_path=base_path
        )
        load_config["value_id"] = value_id

        lc = LoadConfig(**load_config)

        if lc.base_path_input_name not in lc.inputs.keys():
            raise KiaraProcessingException(
                f"Invalid load config: base path '{lc.base_path_input_name}' not part of inputs."
            )

        outputs.set_values(load_config=lc)


class SaveTypeOperationConfig(TypeOperationConfig):
    """Save a dataset into the internal kiara data store."""

    @classmethod
    def retrieve_operation_configs(
        cls, kiara: Kiara
    ) -> typing.Mapping[
        str, typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]
    ]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        # find all KiaraModule subclasses that are relevant for this profile type
        for module_type in kiara.available_module_types:

            m_cls = kiara.get_module_class(module_type=module_type)

            if issubclass(m_cls, SaveValueTypeModule):
                value_types: typing.Iterable[str] = m_cls.get_supported_value_types()

                if "*" in value_types:
                    value_types = kiara.type_mgmt.value_type_names

                for value_type in value_types:

                    mc = {"value_type": value_type}
                    profile_config = {
                        "module_type": module_type,
                        "module_config": mc,
                        "input_name": "value_item"
                        # "value_type": value_type,
                    }
                    all_metadata_profiles.setdefault(value_type, {}).setdefault(
                        "save_value", {}
                    )["data_store"] = profile_config
                    # TODO: validate here?

        return all_metadata_profiles
