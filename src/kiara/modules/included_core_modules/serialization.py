# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import orjson
from pydantic import Field, validator
from typing import Any, Mapping, Type, Union

from kiara.models import KiaraModel
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import SerializedData, Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import (
    DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS,
    KiaraModule,
    ModuleCharacteristics,
    ValueMapSchema,
)
from kiara.registries.models import ModelRegistry


class SerializeConfig(KiaraModuleConfig):

    value_type: str = Field(
        description="The value type of the actual (unserialized) value."
    )
    target_profile: str = Field(
        description="The profile name of the de-serialization result data."
    )
    serialization_profile: str = Field(
        description="The name of the serialization profile used to serialize the source value."
    )

    @validator("value_type")
    def validate_source_type(cls, value):
        if value == "serialization_config":
            raise ValueError(f"Invalid source type: {value}.")
        return value


class DeserializeValueModule(KiaraModule):

    _config_cls = SerializeConfig

    @classmethod
    @abc.abstractmethod
    def retrieve_serialized_value_type(cls) -> str:
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        raise NotImplementedError()

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        value_type = self.get_config_value("value_type")
        return {
            value_type: {
                "type": value_type,
                "doc": "The value object.",
            },
            "deserialization_config": {
                "type": "any",
                "doc": "Serialization-format specific configuration.",
                "optional": True,
            },
        }

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "python_object": {
                "type": "python_object",
                "doc": "The deserialized python object instance.",
            },
        }

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        value_type = self.get_config_value("value_type")
        serialized_value = inputs.get_value_obj(value_type)
        config = inputs.get_value_obj("deserialization_config")

        target_profile = self.get_config_value("target_profile")
        func_name = f"to__{target_profile}"
        func = getattr(self, func_name)

        if config.is_set:
            _config = config.data
        else:
            _config = {}

        result: Any = func(data=serialized_value.serialized_data, **_config)
        outputs.set_value("python_object", result)


class UnpickleModule(DeserializeValueModule):

    _module_type_name = "unpickle.value"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:

        return {"python_object": object}

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "pickle"

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:

        return "any"

    def to__python_object(self, data: SerializedData, **config: Any):

        try:
            import pickle5 as pickle
        except Exception:
            import pickle  # type: ignore

        assert "python_object" in data.get_keys()
        python_object_data = data.get_serialized_data("python_object")
        assert python_object_data.get_number_of_chunks() == 1

        _bytes = list(python_object_data.get_chunks(as_files=False))[0]
        data = pickle.loads(_bytes)

        return data


class LoadBytesModule(DeserializeValueModule):

    _module_type_name = "load.bytes"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": bytes}

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "raw"

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "bytes"

    def to__python_object(self, data: SerializedData, **config: Any) -> bytes:

        chunks = data.get_serialized_data("bytes")
        assert chunks.get_number_of_chunks() == 1
        _chunks = list(chunks.get_chunks(as_files=False))
        assert len(_chunks) == 1
        _chunk: bytes = _chunks[0]  # type: ignore
        return _chunk


class LoadStringModule(DeserializeValueModule):

    _module_type_name = "load.string"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": str}

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "raw"

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "string"

    def to__python_object(self, data: SerializedData, **config: Any) -> str:

        chunks = data.get_serialized_data("string")
        assert chunks.get_number_of_chunks() == 1
        _chunks = list(chunks.get_chunks(as_files=False))
        assert len(_chunks) == 1

        bytes_string: bytes = _chunks[0]  # type: ignore
        return bytes_string.decode("utf-8")


class LoadInternalModel(DeserializeValueModule):

    _module_type_name = "load.internal_model"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": KiaraModel}

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "json"

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "internal_model"

    def to__python_object(self, data: SerializedData, **config: Any) -> KiaraModel:

        chunks = data.get_serialized_data("data")
        assert chunks.get_number_of_chunks() == 1
        _chunks = list(chunks.get_chunks(as_files=False))
        assert len(_chunks) == 1

        bytes_string: bytes = _chunks[0]  # type: ignore
        model_data = orjson.loads(bytes_string)

        model_id: str = data.data_type_config["kiara_model_id"]
        model_registry = ModelRegistry.instance()
        m_cls = model_registry.get_model_cls(kiara_model_id=model_id)
        obj = m_cls(**model_data)
        return obj


class DeserializeJsonConfig(KiaraModuleConfig):

    result_path: Union[str, None] = Field(
        description="The path of the result dictionary to return.", default="data"
    )


class DeserializeFromJsonModule(KiaraModule):

    _module_type_name: str = "deserialize.from_json"
    _config_cls = DeserializeJsonConfig

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "value": {
                "type": "any",
                "doc": "The value object to deserialize the data for.",
            }
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "python_object": {
                "type": "python_object",
                "doc": "The deserialized python object.",
            }
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        value: Value = inputs.get_value_obj("value")
        serialized: SerializedData = value.serialized_data

        chunks = serialized.get_serialized_data(self.get_config_value("result_path"))
        assert chunks.get_number_of_chunks() == 1
        _data = list(chunks.get_chunks(as_files=False))
        assert len(_data) == 1
        _chunk: bytes = _data[0]  # type: ignore

        deserialized = orjson.loads(_chunk)

        outputs.set_value("python_object", deserialized)
