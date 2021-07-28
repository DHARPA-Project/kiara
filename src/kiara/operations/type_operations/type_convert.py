# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig
from kiara.operations.type_operations import TypeOperationConfig


class TypeConversionModuleConfig(KiaraModuleConfig):

    source_type: str = Field(description="The type of the source value.")
    target_type: str = Field(description="The type of the converted value.")


class TypeConversionModule(KiaraModule):

    _config_cls = TypeConversionModuleConfig

    @classmethod
    def get_supported_value_types(cls) -> typing.Set[str]:

        _types = cls._get_supported_value_types()
        if isinstance(_types, str):
            _types = [_types]

        return set(_types)

    @classmethod
    @abc.abstractmethod
    def _get_supported_value_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        pass

    @classmethod
    def get_source_value_types(cls) -> typing.Iterable[str]:

        supported = cls.get_supported_value_types()

        types = []
        for attr_name, attr in cls.__dict__.items():

            if attr_name.startswith("from_") and callable(attr):
                v_type = attr_name[5:]
                if v_type in supported:
                    raise Exception(
                        f"Invalid configuration for type conversion module class {cls.__name__}: conversion source type can't be '{v_type}' because this is already registered as a supported type of the class."
                    )
                types.append(v_type)

        return types

    @classmethod
    def get_target_value_types(cls) -> typing.Iterable[str]:

        supported = cls.get_supported_value_types()

        types = []
        for attr_name, attr in cls.__dict__.items():

            if attr_name.startswith("to_") and callable(attr):
                v_type = attr_name[3:]
                if v_type in supported:
                    raise Exception(
                        f"Invalid configuration for type conversion module class {cls.__name__}: conversion target type can't be '{v_type}' because this is already registered as a supported type of the class."
                    )
                types.append(attr_name[3:])

        return types

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        supported = self.get_supported_value_types()
        source_type = self.get_config_value("source_type")
        target_type = self.get_config_value("target_type")

        if source_type in supported:
            # means this is a 'from supported type' conversion
            if target_type not in self.get_target_value_types():
                raise Exception(
                    f"Can't create input schema for module '{self.type_name}': converstion to target value type '{target_type}' not supported."
                )

        return {
            "value_item": {
                "type": source_type,
                "doc": "The value that needs to be converted.",
            }
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        supported = self.get_supported_value_types()
        source_type = self.get_config_value("source_type")
        target_type = self.get_config_value("target_type")

        if target_type in supported:
            # means this is a 'to supported type' conversion
            if source_type not in self.get_source_value_types():
                raise Exception(
                    f"Can't create output schema for module '{self.type_name}': conversion from source value type '{source_type}' not supported."
                )

        return {
            "value_item": {
                "type": target_type,
                "doc": "The type of the converted value.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        supported_types = self.get_supported_value_types()
        source_type: str = self.get_config_value("source_type")
        target_type: str = self.get_config_value("target_type")

        source: Value = inputs.get_value_obj("value_item")
        if source_type != source.type_name:
            raise KiaraProcessingException(
                f"Invalid type ({source.type_name}) of source value: expected '{source_type}'."
            )

        if source_type in supported_types:
            # means this is a 'to' conversion

            if not hasattr(self, f"to_{target_type}"):
                # this can never happen, I think
                raise Exception(
                    f"Module '{self.type_name}' can't convert '{source_type}' into '{target_type}': missing method 'to_{target_type}'. This is a bug."
                )

            func = getattr(self, f"to_{target_type}")
        elif target_type in supported_types:
            # menas this is a 'from' conversion
            if not hasattr(self, f"from_{source_type}"):
                # this can never happen, I think
                raise Exception(
                    f"Module '{self.type_name}' can't convert '{source_type}' into '{target_type}': missing method 'from_{source_type}'. This is a bug."
                )

            func = getattr(self, f"from_{source_type}")

        converted = func(source)
        outputs.set_value("value_item", converted)


class TypeConversionTypeOperationConfig(TypeOperationConfig):
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

            if issubclass(m_cls, TypeConversionModule):

                value_types: typing.Iterable[str] = m_cls.get_supported_value_types()
                if "*" in value_types:
                    value_types = kiara.type_mgmt.value_type_names

                for value_type in value_types:

                    for target_type in m_cls.get_target_value_types():

                        mod_conf = {
                            "source_type": value_type,
                            "target_type": target_type,
                        }

                        op_config = {
                            "module_type": module_type,
                            "module_config": mod_conf,
                            "input_name": "value_item",
                        }
                        all_metadata_profiles.setdefault(value_type, {}).setdefault(
                            "convert_to", {}
                        )[target_type] = op_config

                    for source_type in m_cls.get_source_value_types():

                        mod_conf = {
                            "source_type": source_type,
                            "target_type": value_type,
                        }

                        op_config = {
                            "module_type": module_type,
                            "module_config": mod_conf,
                            "input_name": "value_item",
                        }
                        all_metadata_profiles.setdefault(source_type, {}).setdefault(
                            "convert_to", {}
                        )[value_type] = op_config

        return all_metadata_profiles
