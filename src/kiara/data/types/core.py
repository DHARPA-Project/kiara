# -*- coding: utf-8 -*-
import json
import typing

from kiara.data.types import ValueType
from kiara.metadata.data import LoadConfig

if typing.TYPE_CHECKING:
    from kiara.data.values import Value, ValueInfo


class ValueLoadConfig(ValueType):
    """A dictionary representing load config for a *kiara* value.

    The load config schema itself can be looked up via the wrapper class  '[LoadConfig][kiara.data.persistence.LoadConfig]'.
    """

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [typing.Mapping, LoadConfig]

    @classmethod
    def type_name(cls):
        return "load_config"

    #     @classmethod
    #     def doc(cls):
    #
    #         desc = cls.desc()
    #         schema = LoadConfig.schema_json(indent=2)
    #
    #         doc = "A load config is a dictionary that contains instructions on how to load a *kiara* value. Those instructions include a *kiara* module name, an optional module config, and values for the inputs to use when running the value. In addition, an output name can be specified, which indicates to *kiara* which of the output fields (if multiple) contains the desired value."
    #
    #         result = f"""{desc}\n\n{doc}\n\nThe schema for a load config is:
    #
    # ```
    # {schema}
    # ```
    # """
    #         return result

    def parse_value(self, value: typing.Any) -> typing.Any:

        if isinstance(value, typing.Mapping):
            _value = LoadConfig(**value)
            return _value

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, LoadConfig):
            raise Exception(f"Invalid type for load config: {type(value)}.")

    def pretty_print_as_renderables(
        self, value: "Value", print_config: typing.Mapping[str, typing.Any]
    ) -> typing.Any:

        data: LoadConfig = value.get_value_data()
        return data.json(indent=2)


class ValueMetadata(ValueType):
    """Metadata of a value that was stored in a kiara data store."""

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [ValueInfo]

    @classmethod
    def type_name(cls):
        return "value_metadata"

    @classmethod
    def parse_value(self, value: typing.Any) -> typing.Any:

        if isinstance(value, typing.Mapping):
            _value = ValueInfo(**value)
            return _value

    @classmethod
    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, ValueInfo):
            raise Exception(f"Invalid type for value_metadata: {type(value)}.")

    def pretty_print_as_renderables(
        self, value: "Value", print_config: typing.Mapping[str, typing.Any]
    ) -> typing.Any:

        data: ValueInfo = value.get_value_data()
        _temp = data.dict()
        md = data.get_metadata_items()
        _temp["metadata"] = md
        return json.dumps(_temp, indent=2)
