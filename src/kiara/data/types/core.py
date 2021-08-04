# -*- coding: utf-8 -*-
import typing

from kiara.data.types import ValueType
from kiara.metadata.data import LoadConfig


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

    @classmethod
    def doc(cls):

        desc = cls.desc()
        schema = LoadConfig.schema_json(indent=2)

        doc = "A load config is a dictionary that contains instructions on how to load a *kiara* value. Those instructions include a *kiara* module name, an optional module config, and values for the inputs to use when running the value. In addition, an output name can be specified, which indicates to *kiara* which of the output fields (if multiple) contains the desired value."

        result = f"""{desc}\n\n{doc}\n\nThe schema for a load config is:

```
{schema}
```
"""
        return result

    def validate(cls, value: typing.Any) -> None:

        if isinstance(value, typing.Mapping):
            _value = LoadConfig(**value)

        if not isinstance(_value, LoadConfig):
            raise Exception(f"Invalid type for load config: {type(value)}.")
