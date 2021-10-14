# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.metadata.data import DeserializeConfig
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message


class SerializeValueModuleConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The type of the source value.")
    serialization_type: str = Field(description="The type of the converted value.")


class SerializeValueModule(KiaraModule):
    """Base class for 'serialize' operations."""

    _config_cls = SerializeValueModuleConfig

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        value_type: str = cls.get_value_type()

        if value_type not in kiara.type_mgmt.value_type_names:
            log_message(
                f"Ignoring serialization operation for source type '{value_type}': type not available"
            )
            return {}

        for serialization_type in cls.get_supported_serialization_types():

            mod_conf = {
                "value_type": value_type,
                "serialization_type": serialization_type,
            }

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": mod_conf,
                "doc": f"Serialize value of type '{value_type}' to '{serialization_type}'.",
            }
            key = f"{value_type}.serialize_to.{serialization_type}"
            if key in all_profiles.keys():
                raise Exception(f"Duplicate profile key: {key}")
            all_profiles[key] = op_config

        return all_profiles

    @classmethod
    @abc.abstractmethod
    def get_value_type(cls) -> str:
        pass

    @classmethod
    def get_supported_serialization_types(cls) -> typing.Iterable[str]:

        serialize_types = []
        for attr_name, attr in cls.__dict__.items():

            if attr_name.startswith("to_") and callable(attr):
                serialize_types.append(attr_name[3:])

        return serialize_types

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        value_type: str = self.get_config_value("value_type")
        # serialization_type: str = self.get_config_value("serialization_type")

        return {
            "value_item": {
                "type": value_type,
                "doc": f"The '{value_type}' value to be serialized.",
            }
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        value_type: str = self.get_config_value("value_type")

        return {
            "value_info": {
                "type": "value_info",
                "doc": "Information about the (original) serialized value (can be used to re-constitute the value, incl. its original id).",
            },
            "deserialize_config": {
                "type": "deserialize_config",
                "doc": f"The config to use to deserialize the value of type '{value_type}'.",
            },
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value_type: str = self.get_config_value("value_type")

        value_obj = inputs.get_value_obj("value_item")

        serialization_type = self.get_config_value("serialization_type")

        if value_type != value_obj.type_name:
            raise KiaraProcessingException(
                f"Invalid type ({value_obj.type_name}) of source value: expected '{value_type}'."
            )

        if not hasattr(self, f"to_{serialization_type}"):
            # this can never happen, I think
            raise Exception(
                f"Module '{self._module_type_id}' can't serialize '{value_type}' to '{serialization_type}': missing method 'to_{serialization_type}'. This is a bug."  # type: ignore
            )

        func = getattr(self, f"to_{serialization_type}")

        serialized = func(value_obj)

        if isinstance(serialized, typing.Mapping):
            serialized = DeserializeConfig(**serialized)

        if not isinstance(serialized, DeserializeConfig):
            raise KiaraProcessingException(
                f"Invalid serialization result type: {type(serialized)}"
            )

        outputs.set_values(
            deserialize_config=serialized, value_info=value_obj.get_info()
        )


class SerializeValueOperationType(OperationType):
    """Operations that serialize data into formats that can be used for data exchange.

    NOT USED YET
    """

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, SerializeValueModule)

    def get_operations_for_value_type(
        self, value_type: str
    ) -> typing.Dict[str, Operation]:
        """Find all operations that serialize the specified type.

        The result dict uses the serialization type as key, and the operation itself as value.
        """

        result: typing.Dict[str, Operation] = {}
        for o_id, op in self.operations.items():
            source_type = op.module_config["value_type"]
            if source_type == value_type:
                target_type = op.module_config["serialization_type"]
                if target_type in result.keys():
                    raise Exception(
                        f"Multiple operations to serialize '{source_type}' to {target_type}"
                    )
                result[target_type] = op

        return result
