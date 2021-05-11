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

import networkx
import networkx as nx
import pyarrow
import typing
from networkx import DiGraph
from pyarrow import Array
from rich.console import Console, ConsoleOptions, RenderResult

from kiara.utils import camel_case_to_snake_case


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

    def __init__(self, **type_config: typing.Any):

        self._type_config: typing.Mapping[str, typing.Any] = type_config

    def parse_value(self, value: typing.Any) -> typing.Mapping[str, typing.Any]:
        """Validate value, and extract metadata.

        Will raise an exception if validation fails.

        Arguments:
            value: a data value
        Returns:
            the metadata of the value
        """

        self.validate(value)
        value_metadata = self.extract_metadata(value)
        return value_metadata

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

    def validate(cls, v: typing.Any) -> None:
        """Overwrite this method to validate data for this type.

        This part is not really implemented yet, but should work in a basic way. Just raise an Exception if something is
        wrong with the data.
        """

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        """Overwrite this method to extract type-specific metadata.

        I haven't thought about metadata schema yet, there is a good chance that each type will have to publish what
        sort of metadata fields (and maybe schemas) it will provide.

        Arguments:
            v: the value
        Returns:
            a metadata dictionary
        """
        return {}

    def serialize(cls, object: typing.Any):
        """TODO, this is still up in the air."""
        raise NotImplementedError(
            f"Type class '{cls}' missing implementation of 'serialize' class method. This is a bug."
        )

    def deserialize(cls, object: typing.Any):
        """TODO, this is also still up in the air."""
        raise NotImplementedError(
            f"Type class '{cls}' missing implementation of 'deserialize' class method. This is a bug."
        )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        yield self.__class__.type_name()


class AnyType(ValueType):
    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        return {"python_cls": get_type_name(v)}


class StringType(ValueType):
    def validate(cls, v: typing.Any) -> None:

        if not isinstance(v, str):
            raise ValueError(f"Invalid type '{type(v)}': string required")

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        return {"python_cls": get_type_name(v)}


class BooleanType(ValueType):
    def validate(cls, v: typing.Any):
        if not isinstance(v, bool):
            # if isinstance(v, str):
            #     if v.lower() in ["true", "yes"]:
            #         v = True
            #     elif v.lower() in ["false", "no"]:
            #         v = False
            #     else:
            #         raise ValueError(f"Can't parse string into boolean: {v}")
            # else:
            raise ValueError(f"Invalid type '{type(v)}' for boolean: {v}")

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        return {"python_cls": get_type_name(v)}


class IntegerType(ValueType):
    def validate(cls, v: typing.Any) -> typing.Any:

        if not isinstance(v, int):
            #     if isinstance(v, str):
            #         try:
            #             v = int(v)
            #         except Exception:
            #             raise ValueError(f"Can't parse string into integer: {v}")
            # else:
            raise ValueError(f"Invalid type '{type(v)}' for integer: {v}")

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        return {"python_cls": get_type_name(v)}


class DictType(ValueType):
    def validate(cls, v: typing.Any) -> typing.Any:

        if not isinstance(v, typing.Mapping):
            raise ValueError(f"Invalid type '{type(v)}', not a mapping.")

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        result = {"keys": list(v.keys()), "python_cls": get_type_name(v)}
        return result


class FloatType(ValueType):
    def validate(cls, v: typing.Any) -> typing.Any:

        if not isinstance(v, float):
            raise ValueError(f"Invalid type '{type(v)}' for float: {v}")

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        return {"python_cls": get_type_name(v)}


class ArrayType(ValueType):
    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        a: Array = v

        metadata = {"type": str(v.type), "arrow_type_id": v.type.id, "length": len(v)}

        metadata["python_cls"] = get_type_name(a)
        return metadata


class DateType(ValueType):
    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        result = {}
        result["python_cls"] = get_type_name(v)

        return result


class TableType(ValueType):
    def validate(cls, v: typing.Any) -> None:
        assert isinstance(v, pyarrow.Table)

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        table: pyarrow.Table = v
        table_schema = {}
        for name in table.schema.names:
            field = table.schema.field(name)
            md = field.metadata
            if not md:
                md = {}
            _type = field.type
            _d = {"type": str(_type), "arrow_type_id": _type.id, "metadata": md}
            table_schema[name] = _d

        return {
            "column_names": table.column_names,
            "schema": table_schema,
            "rows": table.num_rows,
            "python_cls": get_type_name(table),
        }


class NetworkGraphType(ValueType):
    def validate(cls, v: typing.Any) -> typing.Any:

        if not isinstance(v, networkx.Graph):
            raise ValueError(f"Invalid type '{type(v)}' for graph: {v}")
        return v

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        graph: nx.Graph = v
        return {
            "python_cls": get_type_name(graph),
            "directed": isinstance(v, DiGraph),
            "number_of_nodes": len(graph.nodes),
            "number_of_edges": len(graph.edges),
            "density": nx.density(graph),
        }


# class ValueTypesEnum(Enum):
#
#     @classmethod
#     def list_all_types(cls):
#         return list(map(lambda c: c.value, cls))
#
#     def __new__(cls, *args, **kwds):
#         type_name = args[0].type_name()
#         obj = object.__new__(cls)
#         obj._value_ = type_name
#         return obj
#
#     def __init__(self, cls: typing.Type[ValueType]):
#         setattr(self, "cls", cls)
#
#


# class ValueTypes(Enum):
#     """Supported value types.
#
#     It's very early days, so this does not really do anything yet.
#     """
#
#     def __new__(cls, *args, **kwds):
#         value = args[0]["id"]
#         obj = object.__new__(cls)
#         obj._value_ = value
#         return obj
#
#     def __init__(self, type_map: typing.Mapping[str, typing.Any]):
#
#         for k, v in type_map.items():
#             setattr(self, k, v)
#
#     any = {"id": "any", "python": object, "fake_value": fake.pydict}
#     integer = {"id": "integer", "python": int, "fake_value": fake.pyint}
#     string = {"id": "string", "python": str, "fake_value": fake.pystr}
#     dict = {"id": "dict", "python": dict, "fake_value": fake.pydict}
#     boolean = {"id": "boolean", "python": bool, "fake_value": fake.pybool}
#     table = {
#         "id": "table",
#         "python": typing.List[typing.Dict],
#         "fake_value": fake.pydict,
#     }
#     value_items = {
#         "id": "value_items",
#         "python": dict,
#         "fake_value": NotImplemented,
#     }
