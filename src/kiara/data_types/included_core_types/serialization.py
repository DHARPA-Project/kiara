# -*- coding: utf-8 -*-
from deepdiff import DeepHash
from mmh3 import hash_from_buffer
from pydantic import BaseModel, Field, PrivateAttr
from typing import Dict, Optional, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types.internal import InternalType
from kiara.defaults import KIARA_HASH_FUNCTION
from kiara.models.module.manifest import Manifest


class SerializedValueTypeConfigSchema(DataTypeConfig):

    format_name: str = Field(description="The name of the serialization format.")


class DeserializationConfig(Manifest):

    output_name: str = Field(
        description="The name of the field that contains the deserialized value."
    )


class SerializedValueModel(BaseModel):

    deserialization_config: DeserializationConfig = Field(
        description="The configuration for a kiara module that deserializes this value."
    )
    data: Dict[str, bytes] = Field(
        description="One or several byte arrays representing the serialized state of the value."
    )

    _cached_size: Optional[int] = PrivateAttr(default=None)
    _cached_hash: Optional[int] = PrivateAttr(default=None)

    @property
    def serialized_size(self) -> int:

        if self._cached_size is not None:
            return self._cached_size

        size = 0
        for k, v in self.data.items():
            size = size + len(k) + len(v)

        self._cached_size = size
        return self._cached_size

    @property
    def serialized_hash(self) -> int:

        if self._cached_hash is not None:
            return self._cached_hash

        obj = {
            "deserialization_config": self.deserialization_config.dict(),
            "data": {k: hash_from_buffer(v) for k, v in self.data.items()},
        }
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)

        self._cached_hash = h[obj]
        return self._cached_hash

    def __repr__(self):

        return f"{self.__class__.__name__}(deserialization_config={self.deserialization_config}, size={self.serialized_size}, hash={self.serialized_hash})"

    def __str__(self):
        return self.__repr__()


class SerializedValueType(
    InternalType[SerializedValueModel, SerializedValueTypeConfigSchema]
):
    """A data type that contains a serialized representation of a value.

    This is used for transferring/streaming value over the wire, and works on a similar principle as the 'load_config'
    value type.
    """

    @classmethod
    def python_class(cls) -> Type:
        return SerializedValueModel

    @classmethod
    def data_type_config_class(cls) -> Type[DataTypeConfig]:
        return SerializedValueTypeConfigSchema

    def is_immutable(self) -> bool:
        return True

    def calculate_hash(self, value: SerializedValueModel) -> int:
        """Calculate the hash of the value."""

        return value.serialized_hash

    def calculate_size(self, value: SerializedValueModel) -> int:
        return value.serialized_size

    @property
    def format_name(self) -> str:
        return self.type_config.format_name
