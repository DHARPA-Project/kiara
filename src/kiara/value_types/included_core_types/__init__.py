# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc

from typing import TYPE_CHECKING, Any, Generic, Mapping, Type, TypeVar

from pydantic import Field

from kiara.models import KiaraModel
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara.value_types import TYPE_PYTHON_CLS, ValueType, ValueTypeConfigSchema, TYPE_CONFIG_CLS

if TYPE_CHECKING:
    pass


class AnyType(ValueType[object, ValueTypeConfigSchema], Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS]):
    """Any type / No type information."""

    _value_type_name = "any"

    @classmethod
    def python_class(cls) -> Type:
        return object

    @classmethod
    def value_type_config_class(cls) -> Type[ValueTypeConfigSchema]:
        return ValueTypeConfigSchema

    # def is_immutable(self) -> bool:
    #     return False

    def calculate_hash(self, value: TYPE_PYTHON_CLS) -> int:
        raise Exception(
            f"Calculating the hash for type '{self.__class__._value_type_name}' is not supported. If your type inherits from 'any', make sure to implement the 'calculate_hash' method."
        )

    def calculate_size(self, value: TYPE_PYTHON_CLS) -> int:
        raise Exception(
            f"Calculating size for type '{self.__class__._value_type_name}' is not supported. If your type inherits from 'any', make sure to implement the 'calculate_hash' method."
        )

    def pretty_print_as_renderables(
        self, value: "ValueOrm", print_config: Mapping[str, Any]
    ) -> Any:

        data = value.get_value_data()
        return [str(data)]


MODEL_DATA_TYPE = TypeVar("MODEL_DATA_TYPE", bound=KiaraModel)


class ModelDataType(Value, AnyType[KiaraModel, ValueTypeConfigSchema], Generic[MODEL_DATA_TYPE, TYPE_CONFIG_CLS]):

    _value_type_name = None

    @classmethod
    def python_class(cls) -> Type[MODEL_DATA_TYPE]:
        return cls.backing_model_type()

    @classmethod
    @abc.abstractmethod
    def backing_model_type(cls) -> Type[MODEL_DATA_TYPE]:
        pass

    # def is_immutable(self) -> bool:
    #     return True

    def calculate_hash(self, data: MODEL_DATA_TYPE) -> str:
        return data.model_data_hash

class ValueMetadataConfig(ValueTypeConfigSchema):

    metadata_model: str = Field(description="The metadata model.")

class ValueMetadataType(ValueType[ValueMetadata, ValueMetadataConfig]):

    _value_type_name = "value_metadata"

    @classmethod
    def python_class(cls) -> Type[ValueMetadata]:
        return ValueMetadata

    @classmethod
    def value_type_config_class(cls) -> Type[ValueTypeConfigSchema]:
        return ValueMetadataConfig

    def calculate_hash(self, data: ValueMetadata) -> int:
        return data.model_data_hash

    def calculate_size(self, data: ValueMetadata) -> int:
        return data.model_size
