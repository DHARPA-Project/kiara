# -*- coding: utf-8 -*-
import typing
from pydantic import BaseModel
from rich.syntax import Syntax

from kiara.data.types import ValueType
from kiara.data.values import ValueInfo, ValueLineage
from kiara.metadata.data import DeserializeConfig, LoadConfig

if typing.TYPE_CHECKING:
    from kiara.data.values import Value


class AnyType(ValueType):
    """Any type / No type information."""

    _value_type_name = "any"

    def pretty_print_as_renderables(
        self, value: "Value", print_config: typing.Mapping[str, typing.Any]
    ) -> typing.Any:

        data = value.get_value_data()
        return [str(data)]


class KiaraInternalValueType(ValueType):
    def pretty_print_as_renderables(
        self, value: "Value", print_config: typing.Mapping[str, typing.Any]
    ) -> typing.Any:

        _value: BaseModel = value.get_value_data()
        return Syntax(_value.json(exclude_none=True, indent=2), "json")


class ValueInfoData(KiaraInternalValueType):
    """A dictionary representing a kiara ValueInfo object."""

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [typing.Mapping, ValueInfo]

    @classmethod
    def type_name(cls):
        return "value_info"

    def parse_value(self, value: typing.Any) -> typing.Any:

        if isinstance(value, typing.Mapping):
            _value = ValueInfo(**value)
            return _value

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, ValueInfo):
            raise Exception(f"Invalid type for value info: {type(value)}.")


class ValueLineageData(KiaraInternalValueType):
    """A dictionary representing a kiara ValueLineage."""

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [typing.Mapping, ValueLineage]

    @classmethod
    def type_name(cls):
        return "value_lineage"

    def parse_value(self, value: typing.Any) -> typing.Any:

        if isinstance(value, typing.Mapping):
            _value = ValueLineage(**value)
            return _value

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, ValueLineage):
            raise Exception(f"Invalid type for value seed: {type(value)}.")


class DeserializeConfigData(KiaraInternalValueType):
    """A dictionary representing a configuration to deserialize a value.

    This contains all inputs necessary to re-create the value.
    """

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [typing.Mapping, DeserializeConfig]

    @classmethod
    def type_name(cls):
        return "deserialize_config"

    def parse_value(self, value: typing.Any) -> typing.Any:

        if isinstance(value, typing.Mapping):
            _value = DeserializeConfig(**value)
            return _value

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, DeserializeConfig):
            raise Exception(f"Invalid type for deserialize config: {type(value)}.")


class LoadConfigData(KiaraInternalValueType):
    """A dictionary representing load config for a *kiara* value.

    The load config schema itself can be looked up via the wrapper class  '[LoadConfig][kiara.data.persistence.LoadConfig]'.
    """

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [typing.Mapping, LoadConfig]

    @classmethod
    def type_name(cls):
        return "load_config"

    def parse_value(self, value: typing.Any) -> typing.Any:

        if isinstance(value, typing.Mapping):
            _value = LoadConfig(**value)
            return _value

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, LoadConfig):
            raise Exception(f"Invalid type for load config: {type(value)}.")


# class ValueMetadataData(ValueType):
#     """Metadata of a value that was stored in a kiara data store."""
#
#     @classmethod
#     def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
#         return [ValueInfo]
#
#     @classmethod
#     def type_name(cls):
#         return "value_metadata"
#
#     @classmethod
#     def parse_value(self, value: typing.Any) -> typing.Any:
#
#         if isinstance(value, typing.Mapping):
#             _value = ValueInfo(**value)
#             return _value
#
#     @classmethod
#     def validate(cls, value: typing.Any) -> None:
#
#         if not isinstance(value, ValueInfo):
#             raise Exception(f"Invalid type for value_metadata: {type(value)}.")
