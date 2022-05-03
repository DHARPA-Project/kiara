# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
from typing import TYPE_CHECKING, Any, Generic, Mapping, Type, TypeVar

from kiara.data_types import (
    TYPE_CONFIG_CLS,
    TYPE_PYTHON_CLS,
    DataType,
    DataTypeCharacteristics,
    DataTypeConfig,
)
from kiara.defaults import INVALID_HASH_MARKER, SpecialValue
from kiara.models import KiaraModel

if TYPE_CHECKING:
    from kiara.models.values.value import SerializedData, Value

SCALAR_CHARACTERISTICS = DataTypeCharacteristics(
    is_scalar=True, is_json_serializable=True
)


class NoneType(DataType[SpecialValue, DataTypeConfig]):
    """Type indicating a 'None' value"""

    _data_type_name = "none"

    @classmethod
    def python_class(cls) -> Type:
        return SpecialValue

    # def is_immutable(self) -> bool:
    #     return False

    def calculate_hash(self, data: Any) -> str:
        return INVALID_HASH_MARKER

    def calculate_size(self, data: Any) -> int:
        return 0

    def parse_python_obj(self, data: Any) -> SpecialValue:
        return SpecialValue.NO_VALUE

    def reender_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data = value.data
        return str(data.value)


class AnyType(
    DataType[TYPE_PYTHON_CLS, DataTypeConfig], Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS]
):
    """'Any' type, the parent type for most other types.

    This type acts as the parents for all (or at least most) non-internal value types. There are some generic operations
    (like 'persist_value', or 'render_value') which are implemented for this type, so it's descendents have a fallback
    option in case no subtype-specific operations are implemented for it. In general, it is not recommended to use the 'any'
    type as module input or output, but it is possible. Values of type 'any' are not allowed to be persisted (at the moment,
    this might or might not change).
    """

    _data_type_name = "any"

    @classmethod
    def python_class(cls) -> Type:
        return object

    def reender_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data = value.data
        return str(data)


class BytesType(AnyType[bytes, DataTypeConfig]):
    """An array of bytes."""

    _data_type_name = "bytes"

    @classmethod
    def python_class(cls) -> Type:
        return bytes

    def serialize(self, data: bytes) -> "SerializedData":

        _data = {"bytes": {"type": "chunk", "chunk": data, "codec": "raw"}}

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": _data,
            "serialization_profile": "raw",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_name": "load.bytes",
                        "module_config": {
                            "value_type": "bytes",
                            "target_profile": "python_object",
                            "serialization_profile": "raw",
                        },
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def render_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: bytes = value.data
        return data.decode()


class StringType(AnyType[str, DataTypeConfig]):
    """A string."""

    _data_type_name = "string"

    @classmethod
    def python_class(cls) -> Type:
        return str

    def serialize(self, data: str) -> "SerializedData":

        _data = {
            "string": {"type": "chunk", "chunk": data.encode("utf-8"), "codec": "raw"}
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": _data,
            "serialization_profile": "raw",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "load.string",
                        "module_config": {
                            "value_type": "string",
                            "target_profile": "python_object",
                            "serialization_profile": "raw",
                        },
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def _retrieve_characteristics(self) -> DataTypeCharacteristics:
        return SCALAR_CHARACTERISTICS

    def _validate(cls, value: Any) -> None:

        if not isinstance(value, str):
            raise ValueError(f"Invalid type '{type(value)}': string required")

    def render_as__bytes(self, value: "Value", render_config: Mapping[str, Any]):
        value_str: str = value.data
        return value_str.encode()


KIARA_MODEL_CLS = TypeVar("KIARA_MODEL_CLS", bound=KiaraModel)


class KiaraModelValueType(
    AnyType[KIARA_MODEL_CLS, TYPE_CONFIG_CLS], Generic[KIARA_MODEL_CLS, TYPE_CONFIG_CLS]
):
    """A value type that is used internally.

    This type should not be used by user-facing modules and/or operations.
    """

    _data_type_name = None  # type: ignore

    @classmethod
    def data_type_config_class(cls) -> Type[DataTypeConfig]:
        return DataTypeConfig

    @abc.abstractmethod
    def create_model_from_python_obj(self, data: Any) -> KIARA_MODEL_CLS:
        pass

    def parse_python_obj(self, data: Any) -> KIARA_MODEL_CLS:

        if isinstance(data, self.__class__.python_class()):
            return data  # type: ignore

        data = self.create_model_from_python_obj(data)
        return data

    def _validate(self, data: KiaraModel) -> None:

        if not isinstance(data, self.__class__.python_class()):
            raise Exception(
                f"Invalid type '{type(data)}', must be: {self.__class__.python_class().__name__}, or subclass."
            )
