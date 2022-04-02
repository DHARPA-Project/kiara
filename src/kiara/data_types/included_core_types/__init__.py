# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc
from typing import Any, Generic, Mapping, Type, TypeVar

from kiara.data_types import (
    TYPE_CONFIG_CLS,
    TYPE_PYTHON_CLS,
    DataType,
    DataTypeCharacteristics,
    DataTypeConfig,
)
from kiara.defaults import (
    INVALID_HASH_MARKER,
    INVALID_SIZE_MARKER,
    KIARA_HASH_FUNCTION,
    SpecialValue,
)
from kiara.models import KiaraModel
from kiara.models.values.value import Value

SKALAR_CHARACTERISTICS = DataTypeCharacteristics(
    is_skalar=True, is_json_serializable=True
)


class NoneType(DataType[SpecialValue, DataTypeConfig]):
    """'Any' type, the parent type for most other types.

    This type acts as the parents for all (or at least most) non-internal value types. There are some generic operations
    (like 'persist_value', or 'render_value') which are implemented for this type, so it's descendents have a fallback
    option in case no subtype-specific operations are implemented for it. In general, it is not recommended to use the 'any'
    type as module input or output, but it is possible. Values of type 'any' are not allowed to be persisted (at the moment,
    this might or might not change).
    """

    _data_type_name = "none"

    @classmethod
    def python_class(cls) -> Type:
        return SpecialValue

    # def is_immutable(self) -> bool:
    #     return False

    def calculate_hash(self, data: Any) -> int:
        return INVALID_HASH_MARKER
        # raise Exception(
        #     f"Calculating the hash for type '{self.__class__._value_type_name}' is not supported. If your type inherits from 'any', make sure to implement the 'calculate_hash' method."
        # )

    def calculate_size(self, data: Any) -> int:
        return INVALID_SIZE_MARKER
        # raise Exception(
        #     f"Calculating size for type '{self.__class__._value_type_name}' is not supported. If your type inherits from 'any', make sure to implement the 'calculate_hash' method."
        # )

    def parse_python_obj(self, data: Any) -> SpecialValue:
        return SpecialValue.NO_VALUE

    def reender_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data = value.data
        return str(data.value)


class AnyType(
    DataType[object, DataTypeConfig], Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS]
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

    # def is_immutable(self) -> bool:
    #     return False

    def calculate_hash(self, data: Any) -> int:
        return INVALID_HASH_MARKER
        # raise Exception(
        #     f"Calculating the hash for type '{self.__class__._value_type_name}' is not supported. If your type inherits from 'any', make sure to implement the 'calculate_hash' method."
        # )

    def calculate_size(self, data: Any) -> int:
        return INVALID_SIZE_MARKER
        # raise Exception(
        #     f"Calculating size for type '{self.__class__._value_type_name}' is not supported. If your type inherits from 'any', make sure to implement the 'calculate_hash' method."
        # )

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

    def calculate_hash(cls, data: bytes) -> int:
        return KIARA_HASH_FUNCTION(data)

    def calculate_size(self, data: bytes) -> int:
        return len(data)

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

    def is_immutable(self) -> bool:
        return True

    def calculate_size(self, data: str) -> int:
        return len(data)

    def calculate_hash(cls, data: str) -> int:
        return KIARA_HASH_FUNCTION(data)

    def _retrieve_characteristics(self) -> DataTypeCharacteristics:
        return SKALAR_CHARACTERISTICS

    def validate(cls, value: Any) -> None:

        if not isinstance(value, str):
            raise ValueError(f"Invalid type '{type(value)}': string required")

    def render_as__bytes(self, value: Value, render_config: Mapping[str, Any]):
        value_str: str = value.data
        return value_str.encode()


KIARA_MODEL_CLS = TypeVar("KIARA_MODEL_CLS", bound=KiaraModel)


class KiaraModelValueType(
    AnyType[KiaraModel, DataTypeConfig], Generic[KIARA_MODEL_CLS, TYPE_CONFIG_CLS]
):
    """A value type that is used internally.

    This type should not be used by user-facing modules and/or operations.
    """

    _data_type_name = None  # type: ignore

    @classmethod
    def data_type_config_class(cls) -> Type[TYPE_CONFIG_CLS]:
        return DataTypeConfig

    @abc.abstractmethod
    def create_model_from_python_obj(self, data: Any) -> KIARA_MODEL_CLS:
        pass

    def parse_python_obj(self, data: Any) -> KIARA_MODEL_CLS:

        if isinstance(data, self.__class__.python_class()):
            return data

        data = self.create_model_from_python_obj(data)
        return data

    def _validate(self, data: KiaraModel) -> None:

        if not isinstance(data, self.__class__.python_class()):
            raise Exception(
                f"Invalid type '{type(data)}', must be: {self.__class__.python_class().__name__}, or subclass."
            )