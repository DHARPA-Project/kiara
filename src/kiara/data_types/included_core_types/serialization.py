# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


from typing import Any, Mapping, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types.internal import InternalType
from kiara.defaults import INVALID_HASH_MARKER, INVALID_SIZE_MARKER
from kiara.models.values.value import SerializedValue, Value

# class SerializedValueTypeConfigSchema(DataTypeConfig):
#
#     serialization_profile: str = Field(description="The name of the serialization profile.")
#
#
# class SerializedDataType(
#     InternalType[SerializedValue, SerializedValueTypeConfigSchema]
# ):
#     """A data type that contains a serialized representation of a value.
#
#     This is used for transferring/streaming value over the wire, and works on a similar principle as the 'load_config'
#     value type.
#     """
#
#     @classmethod
#     def python_class(cls) -> Type:
#         return SerializedValue
#
#     @classmethod
#     def data_type_config_class(cls) -> Type[SerializedValueTypeConfigSchema]:
#         return SerializedValueTypeConfigSchema
#
#     def parse_python_obj(self, data: Any) -> SerializedData:
#
#         if isinstance(data, Mapping):
#             data = SerializedValue(**data)
#
#         return data
#
#     def _validate(self, value: SerializedValue) -> None:
#
#         if not isinstance(value, SerializedValue):
#             raise ValueError(f"Invalid value type: {type(value)}")
#
#     def calculate_hash(self, data: SerializedValue) -> int:
#         """Calculate the hash of the value."""
#
#         return data.serialized_hash
#
#     def calculate_size(self, data: SerializedValue) -> int:
#         return data.size
#
#     @property
#     def serialization_profile(self) -> str:
#         return self.type_config.serialization_profile
#
#     def render_as__terminal_renderable(self, value: Value, render_config: Mapping[str, Any]):
#
#         s_val: SerializedValue = value.data
#         return s_val.create_renderable(**render_config)


class PythonObjectType(InternalType[object, DataTypeConfig]):
    """A data type that contains a serialized representation of a value.

    This is used for transferring/streaming value over the wire, and works on a similar principle as the 'load_config'
    value type.
    """

    @classmethod
    def python_class(cls) -> Type:
        return object

    def parse_python_obj(self, data: Any) -> object:
        return data

    def calculate_hash(self, data: SerializedValue) -> int:
        """Calculate the hash of the value."""
        return INVALID_HASH_MARKER

    def calculate_size(self, data: SerializedValue) -> int:
        return INVALID_SIZE_MARKER

    def render_as__terminal_renderable(
        self, value: Value, render_config: Mapping[str, Any]
    ):

        return str(value.data)
