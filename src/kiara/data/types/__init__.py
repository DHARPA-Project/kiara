# -*- coding: utf-8 -*-
import datetime
import networkx
import networkx as nx
import pyarrow
import typing
from dateutil import parser
from deepdiff import DeepHash
from enum import Enum
from faker import Faker
from networkx import DiGraph
from rich.console import Console, ConsoleOptions, RenderResult

from kiara.utils import camel_case_to_snake_case

fake = Faker()


class ValueHashMarker(Enum):

    NO_VALUE = "-- no_value --"
    DEFERRED = "-- deferred --"
    NO_HASH = "-- no_hash --"


def get_type_name(obj: typing.Any):

    if obj.__class__.__module__ == "builtins":
        return obj.__class__.__name__
    else:
        return f"{obj.__class__.__module__}.{obj.__class__.__name__}"


class ValueType(object):
    def __init__(self, **type_config: typing.Any):

        self._type_config: typing.Mapping[str, typing.Any] = type_config

    def import_value(
        self, value: typing.Any
    ) -> typing.Tuple[
        typing.Any, typing.Mapping[str, typing.Any], typing.Union[ValueHashMarker, int]
    ]:

        assert value is not None

        parsed = self.parse_value(value)
        if parsed is None:
            parsed = value
        self.validate(parsed)
        value_metadata = self.extract_type_metadata(parsed)
        if self.defer_hash_calc():
            _hash: typing.Union[ValueHashMarker, int] = ValueHashMarker.DEFERRED
        else:
            _hash = self.calculate_value_hash(value)
        metadata = {"type": value_metadata, "python": {"cls": get_type_name(parsed)}}
        return (parsed, metadata, _hash)

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

    @classmethod
    def type_name(cls):

        cls_name = cls.__name__
        if cls_name.lower().endswith("type"):
            cls_name = cls_name[0:-4]

        type_name = camel_case_to_snake_case(cls_name)
        return type_name

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

    def serialize(cls, value: typing.Any):
        raise NotImplementedError(
            f"Type class '{cls}' missing implementation of 'serialize' class method. This is a bug."
        )

    def deserialize(cls, value: typing.Any):
        raise NotImplementedError(
            f"Type class '{cls}' missing implementation of 'deserialize' class method. This is a bug."
        )

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
        result = {"keys": list(value.keys()), "value_types": list(value_types)}
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


class ArrayType(ValueType):
    """An Apache arrow array."""

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:

        metadata = {
            "item_type": str(value.type),
            "arrow_type_id": value.type.id,
            "length": len(value),
        }
        return metadata


class DateType(ValueType):
    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:

        return hash(value)

    def parse_value(self, v: typing.Any) -> typing.Any:

        if isinstance(v, str):
            d = parser.parse(v)
            return d

        return None

    def validate(cls, value: typing.Any):
        assert isinstance(value, datetime.datetime)


class TableType(ValueType):
    def validate(cls, value: typing.Any) -> None:
        assert isinstance(value, pyarrow.Table)

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:
        table: pyarrow.Table = value
        table_schema = {}
        for name in table.schema.names:
            field = table.schema.field(name)
            md = field.metadata
            if not md:
                md = {}
            _type = field.type
            _d = {"item_type": str(_type), "arrow_type_id": _type.id, "metadata": md}
            table_schema[name] = _d

        return {
            "column_names": table.column_names,
            "schema": table_schema,
            "rows": table.num_rows,
        }


class NetworkGraphType(ValueType):
    def validate(cls, value: typing.Any) -> typing.Any:

        if not isinstance(value, networkx.Graph):
            raise ValueError(f"Invalid type '{type(value)}' for graph: {value}")
        return value

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:

        graph: nx.Graph = value
        return {
            "directed": isinstance(value, DiGraph),
            "number_of_nodes": len(graph.nodes),
            "number_of_edges": len(graph.edges),
            "density": nx.density(graph),
        }
