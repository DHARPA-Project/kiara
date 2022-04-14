# -*- coding: utf-8 -*-
import abc
from pydantic import Field, validator
from typing import Any, Dict, Iterable, Mapping, Union

from kiara import KiaraModule
from kiara.data_types.included_core_types.serialization import (
    DeserializationConfig,
    SerializedValueModel,
)
from kiara.defaults import ANY_TYPE_NAME, SERIALIZED_DATA_TYPE_NAME
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import Value, ValueSet
from kiara.models.values.value_schema import ValueSchema


class SerializeConfig(KiaraModuleConfig):

    source_type: str = Field(description="The value type of the source.")

    @validator("source_type")
    def validate_source_type(cls, value):
        if value == "serialization_config":
            raise ValueError(f"Invalid source type: {value}.")
        return value


class SerializeValueModule(KiaraModule):

    _config_cls = SerializeConfig

    @classmethod
    def retrieve_supported_source_types(cls) -> Iterable[str]:

        result = []
        for attr in dir(cls):
            if attr.startswith("from__"):
                result.append(attr[6:])
        return result

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")

        return {
            source_type: {"type": source_type, "doc": "The value to serialize."},
            "serialization_config": {
                "type": "any",
                "doc": "Serialization-format specific configuration.",
                "optional": True,
            },
        }

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "serialized_value": {
                "type": SERIALIZED_DATA_TYPE_NAME,
                "type_config": {"format_name": self.get_serialization_format_name()},
                "doc": "The value in serialized form.",
            }
        }

    @abc.abstractmethod
    def get_serialization_format_name(self) -> str:
        pass

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value = inputs.get_value_obj(self.get_config_value("source_type"))
        config = inputs.get_value_obj("serialization_config")

        func_name = f"from__{self.get_config_value('source_type')}"
        func = getattr(self, func_name)

        if config.is_set:
            _config = config.data
        else:
            _config = {}

        result: SerializedValueModel = func(value=value, config=_config)
        outputs.set_value("serialized_value", result)


class PickleModule(SerializeValueModule):

    _module_type_name = "value.serialize.pickle"

    def get_serialization_format_name(self):
        return "pickle"

    def from__any(self, value: Value, config: Dict[str, Any]):
        """Serialize any Python object into a bytes array using 'pickle'."""

        import pickle5 as pickle

        pickled = pickle.dumps(value.data, protocol=5)
        data = {"value": pickled}

        data_type_name = value.data_type_name

        deserialization_config: Dict[str, Any] = {
            "module_type": "value.serialize.pickle",
            "module_config": {"target_type": data_type_name},
            "output_name": data_type_name,
        }
        ser_val = SerializedValueModel(
            data=data,
            deserialization_config=DeserializationConfig.construct(
                **deserialization_config
            ),
        )
        return ser_val


class UnpickleConfig(KiaraModuleConfig):

    target_type: str = Field(
        description="The type of the value to unpickle.", default=ANY_TYPE_NAME
    )


class UnpickleModule(KiaraModule):

    _module_type_name = "value.serialize.unpickle"
    _config_cls = UnpickleConfig

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        target_type = self.get_config_value("target_type")
        return {
            "bytes": {
                "type": "bytes",
                "doc": f"The serialized bytes of the '{target_type}' value.",
            }
        }

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        target_type = self.get_config_value("target_type")
        return {
            target_type: {
                "type": target_type,
                "doc": "The type of the value to unpickle.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet):

        import pickle5 as pickle

        target_type = self.get_config_value("target_type")
        _bytes = inputs.get_value_data("bytes")

        data = pickle.loads(_bytes)

        outputs.set_value(target_type, data)
