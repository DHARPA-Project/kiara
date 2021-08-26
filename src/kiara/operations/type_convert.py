# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message


class TypeConversionModuleConfig(ModuleTypeConfigSchema):

    source_type: str = Field(description="The type of the source value.")
    target_type: str = Field(description="The type of the converted value.")


class ConvertValueModule(KiaraModule):

    _config_cls = TypeConversionModuleConfig

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        value_types: typing.Iterable[str] = cls.get_supported_value_types()
        if "*" in value_types:
            value_types = kiara.type_mgmt.value_type_names

        for value_type in value_types:

            if value_type not in kiara.type_mgmt.value_type_names:
                log_message(
                    f"Ignoring type convert operation for source type '{value_type}': type not available"
                )

            for target_type in cls.get_target_value_types():

                if target_type not in kiara.type_mgmt.value_type_names:
                    log_message(
                        f"Ignoring type convert operation for target type '{target_type}': type not available"
                    )

                mod_conf = {
                    "source_type": value_type,
                    "target_type": target_type,
                }

                op_config = {
                    "module_type": cls._module_type_id,  # type: ignore
                    "module_config": mod_conf,
                    "doc": f"Convert value of type '{value_type}' to type '{target_type}'.",
                }
                key = f"{value_type}.convert_to.{target_type}"
                if key in all_metadata_profiles.keys():
                    raise Exception(f"Duplicate profile key: {key}")
                all_metadata_profiles[key] = op_config
                # key = f"{target_type}.convert_from.{value_type}"
                # if key in all_metadata_profiles.keys():
                #     raise Exception(f"Duplicate profile key: {key}")
                # all_metadata_profiles[key] = op_config

            for source_type in cls.get_source_value_types():

                mod_conf = {
                    "source_type": source_type,
                    "target_type": value_type,
                }

                op_config = {
                    "module_type": cls._module_type_id,  # type: ignore
                    "module_config": mod_conf,
                    "doc": f"Convert value of type '{source_type}' to type '{value_type}'.",
                }
                # key = f"{value_type}.convert_from.{target_type}"   # type: ignore
                # if key in all_metadata_profiles.keys():
                #     raise Exception(f"Duplicate profile key: {key}")
                # all_metadata_profiles[key] = op_config
                key = f"{source_type}.convert_to.{value_type}"
                if key in all_metadata_profiles.keys():
                    raise Exception(f"Duplicate profile key: {key}")
                all_metadata_profiles[key] = op_config

        return all_metadata_profiles

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
                    f"Can't create input schema for module '{self._module_type_id}': converstion to target value type '{target_type}' not supported."  # type: ignore
                )

        return {
            "value_item": {
                "type": source_type,
                "doc": f"The '{source_type}' value to be converted.",
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
                    f"Can't create output schema for module '{self._module_type_id}': conversion from source value type '{source_type}' not supported."  # type: ignore
                )

        return {
            "value_item": {
                "type": target_type,
                "doc": f"The converted '{source_type}' value as '{target_type}'.",
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
                    f"Module '{self._module_type_id}' can't convert '{source_type}' into '{target_type}': missing method 'to_{target_type}'. This is a bug."  # type: ignore
                )

            func = getattr(self, f"to_{target_type}")
        elif target_type in supported_types:
            # menas this is a 'from' conversion
            if not hasattr(self, f"from_{source_type}"):
                # this can never happen, I think
                raise Exception(
                    f"Module '{self._module_type_id}' can't convert '{source_type}' into '{target_type}': missing method 'from_{source_type}'. This is a bug."  # type: ignore
                )

            func = getattr(self, f"from_{source_type}")

        converted = func(source)
        outputs.set_value("value_item", converted)


class ConvertValueOperationType(OperationType):
    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, ConvertValueModule)

    def get_operations_for_source_type(
        self, value_type: str
    ) -> typing.Dict[str, Operation]:
        """Find all operations that transform from the specified type.

        The result dict uses the target type of the conversion as key, and the operation itself as value.
        """

        result: typing.Dict[str, Operation] = {}
        for o_id, op in self.operation_configs.items():
            source_type = op.module_config["source_type"]
            if source_type == value_type:
                target_type = op.module_config["target_type"]
                if target_type in result.keys():
                    raise Exception(
                        f"Multiple operations to transform from '{source_type}' to {target_type}"
                    )
                result[target_type] = op

        return result

    def get_operations_for_target_type(
        self, value_type: str
    ) -> typing.Dict[str, Operation]:
        """Find all operations that transform to the specified type.

        The result dict uses the source type of the conversion as key, and the operation itself as value.
        """

        result: typing.Dict[str, Operation] = {}
        for o_id, op in self.operation_configs.items():
            target_type = op.module_config["target_type"]
            if target_type == value_type:
                source_type = op.module_config["source_type"]
                if source_type in result.keys():
                    raise Exception(
                        f"Multiple operations to transform from '{source_type}' to {target_type}"
                    )
                result[source_type] = op
        return result
