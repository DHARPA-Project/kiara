# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
from pydantic import Field, validator
from typing import Any, Mapping, Type, Union

from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import SerializedData, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule


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
    def retrieve_source_value_type(cls) -> str:
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

    _module_type_name = "value.unpickle"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:

        return {"object": object}

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "pickle"

    @classmethod
    def retrieve_source_value_type(cls) -> str:

        return "any"

    def to__object(self, data: SerializedData, **config: Any):

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


# class SerializeValueModule(KiaraModule):
#
#     _config_cls = SerializeConfig
#
#     @classmethod
#     def retrieve_supported_source_types(cls) -> Iterable[str]:
#
#         result = []
#         for attr in dir(cls):
#             if attr.startswith("from__"):
#                 result.append(attr[6:])
#         return result
#
#     def create_inputs_schema(
#         self,
#     ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:
#
#         source_type = self.get_config_value("value_type")
#
#         return {
#             source_type: {"type": source_type, "doc": "The value to serialize."},
#             "serialization_config": {
#                 "type": "any",
#                 "doc": "Serialization-format specific configuration.",
#                 "optional": True,
#             },
#         }
#
#     def create_outputs_schema(
#         self,
#     ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:
#
#         return {
#             "serialized_value": {
#                 "type": SERIALIZED_DATA_TYPE_NAME,
#                 "type_config": {"serialization_profile": self.get_serialization_profile_name()},
#                 "doc": "The value in serialized form.",
#             }
#         }
#
#     @abc.abstractmethod
#     def get_serialization_profile_name(self) -> str:
#         pass
#
#     def process(self, inputs: ValueMap, outputs: ValueMap) -> None:
#
#         source_type = self.get_config_value("value_type")
#         value = inputs.get_value_obj(source_type)
#         config = inputs.get_value_obj("serialization_config")
#
#         func_name = f"from__{source_type}"
#         func = getattr(self, func_name)
#
#         if config.is_set:
#             _config = config.data
#         else:
#             _config = {}
#
#         result: Union[Mapping, SerializedValue] = func(value=value, config=_config)
#         outputs.set_value("serialized_value", result)
#

#
#
# class PickleModule(SerializeValueModule):
#
#     _module_type_name = "value.serialize.pickle"
#
#     def get_serialization_profile_name(self):
#         return "pickle"
#
#     def from__any(self, value: Value, config: Dict[str, Any]) -> SerializedValue:
#         """Serialize any Python object into bytes using 'pickle'."""
#
#         try:
#             import pickle5 as pickle
#         except:
#             import pickle
#
#         pickled = pickle.dumps(value.data, protocol=5)
#         data = {"python_object": {
#             "type": "chunk",
#             "chunk": pickled
#         }}
#
#         serialized_data = {
#             "data_type": value.value_schema.type,
#             "data_type_config": value.value_schema.type_config,
#             "serialization_profile": self.get_serialization_profile_name(),
#             "environment": {},
#             "data": data
#         }
#
#         serialized = SerializedValue(**serialized_data)
#
#         return serialized
#
#
# class SerializeInternalModelModule(SerializeValueModule):
#     """Persist internally used model data."""
#
#     _module_type_name = "internal_model.serialize.as.json"
#
#     def get_serialization_profile_name(self) -> str:
#         return "json"
#
#     def from__internal_model(
#         self, value: Value, config: Mapping[str, Any]
#     ) -> SerializedValue:
#         """Persist internally used model data as a json file."""
#
#         try:
#             value_metadata: KiaraModel = value.data
#             python_class = PythonClass.from_class(value_metadata.__class__)
#
#             data = {
#                 "internal_model": {
#                     "type": "inline",
#                     "inline_data": {
#                         "data": value_metadata.dict(),
#                         "python_class": python_class.dict()
#                     }
#                 }
#             }
#
#             serialized_data = {
#                 "data_type": value.data_type_name,
#                 "data_type_config": value.data_type_config,
#                 "serialization_profile": self.get_serialization_profile_name(),
#                 "environment": {},
#                 "data": data
#             }
#
#             serialized = SerializedValue(**serialized_data)
#             return serialized
#         except Exception as e:
#             raise KiaraProcessingException(
#                 f"Can't serialize value of type '{value.value_schema.type}' to json: {e}."
#             )
#
#
