# -*- coding: utf-8 -*-

"""This is the base module that contains everything data type-related in *kiara*.

I'm still not 100% sure how to best implement the *kiara* type system, there are several ways it could be done, for example
based on Python type-hints, using JSON-schema, Avro (which is my 2nd favourite option), as well as by implementing a
custom type-class hierarchy. Which is what I have choosen to try first. For now, it looks like it'll work out,
but there is a chance requirements I haven't forseen will crop up that could make this become ugly.

Anyway, the way it works (for now) is that *kiara* comes with a set of often used types (the standard set of: scalars,
list, dict, table & array, etc.) which each come with 2 functions that can serialize and deserialize values of that
type in a persistant fashion -- which could be storing as a file on disk, or as a cell/row in a database. Those functions
will most likley be *kiara* modules themselves, with even more restricted input/output type options.

In addition, packages that contain modules can implement their own, custom types, if suitable ones are not available in
core-*kiara*. Those can either be 'serialized/deserialized' into *kiara*-native types (which in turn will serialize them
using their own serializing functions), or will have to implement custom serializing functionality (which will probably
be discouraged, since this might not be trivial and there are quite a few things to consider).

"""

import typing
from deepdiff import DeepHash
from enum import Enum
from rich.console import Console, ConsoleOptions, RenderResult

from kiara.data.persistence import LoadConfig
from kiara.utils import camel_case_to_snake_case

if typing.TYPE_CHECKING:
    from kiara.data.values import Value


class ValueHashMarker(Enum):

    NO_VALUE = "-- no_value --"
    DEFERRED = "-- deferred --"
    NO_HASH = "-- no_hash --"


def get_type_name(obj: typing.Any):
    """Utility function to get a pretty string from the class of an object."""

    if obj.__class__.__module__ == "builtins":
        return obj.__class__.__name__
    else:
        return f"{obj.__class__.__module__}.{obj.__class__.__name__}"


class ValueType(object):
    """Base class that all *kiara* types must inherit from.

    *kiara* types have 3 main responsibilities:

     - serialize into / deserialize from persistent state
     - data validation
     -  metadata extraction

     Serializing being the arguably most important of those, because without most of the data management features of
     *kiara* would be impossible. Validation should not require any explanation. Metadata extraction is important, because
     that metadata will be available to other components of *kiara* (or frontends for it), without them having to request
     the actual data. That will hopefully make *kiara* very efficient in terms of memory management, as well as data
     transfer and I/O. Ideally, the actual data (bytes) will only be requested at the last possible moment. For example when a
     module needs the input data to do processing on it -- and even then it might be that it only requests a part of the
     data, say a single column of a table. Or when a frontend needs to display/visualize the data.
    """

    @classmethod
    def type_name(cls):
        """Return the name/alias of this type.

        This is the name modules will use in the 'type' field when they create their input/output schemas.

        Returns:
            the type alias
        """

        cls_name = cls.__name__
        if cls_name.lower().endswith("type"):
            cls_name = cls_name[0:-4]

        type_name = camel_case_to_snake_case(cls_name)
        return type_name

    @classmethod
    def conversions(
        self,
    ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
        """Return a dictionary of configuration for modules that can transform this type.

        The name of the transformation is the key of the result dictionary, the configuration is a module configuration
        (dictionary wth 'module_type' and optional 'module_config', 'input_name' and 'output_name' keys).
        """

        return {"string": {"module_type": "string.pretty_print", "input_name": "item"}}

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:
        return None

    @classmethod
    def python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return None

    @classmethod
    def save_config(cls) -> typing.Optional[typing.Mapping[str, typing.Any]]:
        return None

    def __init__(self, **type_config: typing.Any):

        self._type_config: typing.Mapping[str, typing.Any] = type_config
        self._transformations: typing.Optional[
            typing.Mapping[str, typing.Mapping[str, typing.Any]]
        ] = None

    def import_value(
        self, value: typing.Any
    ) -> typing.Tuple[typing.Any, typing.Union[ValueHashMarker, int]]:

        assert value is not None

        parsed = self.parse_value(value)
        if parsed is None:
            parsed = value
        self.validate(parsed)
        if self.defer_hash_calc():
            _hash: typing.Union[ValueHashMarker, int] = ValueHashMarker.DEFERRED
        else:
            _hash = self.calculate_value_hash(value)

        return (parsed, _hash)

    def defer_hash_calc(self) -> bool:
        """Return a recommendation whether to defer the calculation of the hash of a value of this type.

        This is useful to distinguish between types where calculating the hash is trivial, and the overhead of a
        round-trip to the data registry would be more expensive than just calculating the hash on the spot and store
        it in the value metadata right now.
        """
        return True

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        """Calculate the hash of this value.

        If a hash can't be calculated, or the calculation of a type is not implemented (yet), this will return ``ValueHashMarker.NO_HASH``.
        """
        return ValueHashMarker.NO_HASH

    def parse_value(self, value: typing.Any) -> typing.Any:
        """Parse a value into a supported python type.

        This exists to make it easier to do trivial conversions (e.g. from a date string to a datetime object).
        If you choose to overwrite this method, make 100% sure that you don't change the meaning of the value, and try to
        avoid adding or removing information from the data (e.g. by changing the resolution of a date).

        Arguments:
            v: the value

        Returns:
            'None', if no parsing was done and the original value should be used, otherwise return the parsed Python object
        """
        return None

    def validate(cls, value: typing.Any) -> None:
        pass

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:
        return {}

    def save(
        self,
        value: "Value",
        write_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ):
        raise NotImplementedError()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        yield self.__class__.type_name()


class AnyType(ValueType):

    pass


class StringType(ValueType):
    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        return hash(value)

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, str):
            raise ValueError(f"Invalid type '{type(value)}': string required")


class BooleanType(ValueType):
    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        return hash(value)

    def validate(cls, value: typing.Any):
        if not isinstance(value, bool):
            # if isinstance(v, str):
            #     if v.lower() in ["true", "yes"]:
            #         v = True
            #     elif v.lower() in ["false", "no"]:
            #         v = False
            #     else:
            #         raise ValueError(f"Can't parse string into boolean: {v}")
            # else:
            raise ValueError(f"Invalid type '{type(value)}' for boolean: {value}")


class IntegerType(ValueType):
    """An integer."""

    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        return hash(value)

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, int):
            #     if isinstance(v, str):
            #         try:
            #             v = int(v)
            #         except Exception:
            #             raise ValueError(f"Can't parse string into integer: {v}")
            # else:
            raise ValueError(f"Invalid type '{type(value)}' for integer: {value}")


class FloatType(ValueType):
    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        return hash(value)

    def validate(cls, value: typing.Any) -> typing.Any:

        if not isinstance(value, float):
            raise ValueError(f"Invalid type '{type(value)}' for float: {value}")


class DictType(ValueType):
    """A dict-like object."""

    def defer_hash_calc(self) -> bool:
        return True

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:

        dh = DeepHash(value)
        return dh[value]

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, typing.Mapping):
            raise ValueError(f"Invalid type '{type(value)}', not a mapping.")

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:
        value_types = set()
        for val in value.values():
            value_types.add(get_type_name(val))
        result = {"keys": list(value.keys()), "value_types.py": list(value_types)}
        return result


class ListType(ValueType):
    """A list-like object."""

    def defer_hash_calc(self) -> bool:
        return True

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:

        dh = DeepHash(value)
        return dh[value]

    def validate(cls, value: typing.Any) -> None:

        assert isinstance(value, typing.Iterable)

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:

        metadata = {"length": len(value)}
        return metadata


class ValueLoadConfig(ValueType):
    @classmethod
    def python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [typing.Mapping, LoadConfig]

    @classmethod
    def type_name(cls):
        return "load_config"

    def validate(cls, value: typing.Any) -> None:

        if isinstance(value, typing.Mapping):
            _value = LoadConfig(**value)

        if not isinstance(_value, LoadConfig):
            raise Exception(f"Invalid type for load config: {type(value)}.")
