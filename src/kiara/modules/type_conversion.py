# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfig


class TypeConversionModuleConfig(ModuleTypeConfig):

    source_type: str = Field(description="The source type.")
    target_type: str = Field(description="The target type.")


class OldTypeConversionModule(KiaraModule):

    _config_cls = TypeConversionModuleConfig

    @classmethod
    @abc.abstractmethod
    def _get_supported_source_types(self) -> typing.Union[typing.Iterable[str], str]:
        pass

    @classmethod
    @abc.abstractmethod
    def _get_target_types(self) -> typing.Union[typing.Iterable[str], str]:
        pass

    @classmethod
    def get_supported_source_types(self) -> typing.Set[str]:

        _types: typing.Iterable[str] = self._get_supported_source_types()
        if isinstance(_types, str):
            _types = [_types]

        if "config" in _types:
            raise Exception("Invalid source type, type name 'config' is invalid.")
        return set(_types)

    @classmethod
    def get_supported_target_types(self) -> typing.Set[str]:

        _types: typing.Iterable[str] = self._get_target_types()
        if isinstance(_types, str):
            _types = [_types]
        return set(_types)

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

    @property
    def source_type(self) -> str:
        data_type = self.get_config_value("source_type")
        supported = self.get_supported_source_types()
        if "*" not in supported and data_type not in supported:
            raise ValueError(
                f"Invalid module configuration, source type '{data_type}' not supported. Supported types: {', '.join(self.get_supported_source_types())}."
            )

        return data_type

    @property
    def target_type(self) -> str:
        data_type = self.get_config_value("target_type")
        if data_type not in self.get_supported_target_types():
            raise ValueError(
                f"Invalid module configuration, target type '{data_type}' not supported. Supported types: {', '.join(self.get_supported_target_types())}."
            )

        return data_type

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "source_value": {
                "type": self.source_type,
                "doc": f"A value of type '{self.source_type}'.",
            },
            "config": {
                "type": "dict",
                "doc": "The configuration for the transformation.",
                "optional": True,
            },
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {
            "target_value": {
                "type": self.target_type,
                "doc": f"A value of type '{self.target_type}'.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value = inputs.get_value_obj("source_value")
        if value.value_schema.type != self.source_type:
            raise KiaraProcessingException(
                f"Can't convert value of source type '{value.value_schema.type}'. Expected type '{self.source_type}'."
            )
        config = inputs.get_value_data("config")
        if config is None:
            config = {}

        target_value = self.convert(value=value, config=config)
        # TODO: validate value?
        outputs.set_value("target_value", target_value)

    @abc.abstractmethod
    def convert(
        self, value: Value, config: typing.Mapping[str, typing.Any]
    ) -> typing.Any:
        pass
