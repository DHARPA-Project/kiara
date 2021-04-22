# -*- coding: utf-8 -*-
import networkx
import networkx as nx
import pyarrow
import typing
from faker import Faker
from networkx import DiGraph
from rich.console import Console, ConsoleOptions, RenderResult

from kiara.utils import camel_case_to_snake_case

fake = Faker()


class ValueType(object):
    def __init__(self, **type_config: typing.Any):

        self._type_config: typing.Mapping[str, typing.Any] = type_config

    def parse_value(self, value: typing.Any) -> typing.Mapping[str, typing.Any]:
        value = self.validate(value)
        value_metadata = self.extract_metadata(value)
        return value_metadata

    @classmethod
    def type_name(cls):

        cls_name = cls.__name__
        if cls_name.lower().endswith("type"):
            cls_name = cls_name[0:-4]

        type_name = camel_case_to_snake_case(cls_name)
        return type_name

    def validate(cls, v: typing.Any) -> None:
        pass

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        return {}

    def serialize(cls, object: typing.Any):
        raise NotImplementedError(
            f"Type class '{cls.__name__}' missing implementation of 'serialize' class method. This is a bug."
        )

    def deserialize(cls, object: typing.Any):
        raise NotImplementedError(
            f"Type class '{cls.__name__}' missing implementation of 'deserialize' class method. This is a bug."
        )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        yield self.__class__.type_name()


class AnyType(ValueType):
    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        return {"python_cls": str(type(v))}


class StringType(ValueType):
    def validate(cls, v: typing.Any) -> None:

        if not isinstance(v, str):
            raise ValueError(f"Invalid type '{type(v)}': string required")

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        return {"python_cls": "string"}


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
        return {"python_cls": "bool"}


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
        return {"python_cls": "int"}


class ArrayType(ValueType):
    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        a: typing.List = v
        return {"length": len(a)}


class TableType(ValueType):
    def validate(cls, v: typing.Any) -> None:
        assert isinstance(v, pyarrow.Table)

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        table: pyarrow.Table = v
        return {"column_names": table.column_names, "rows": table.num_rows}


class NetworkGraphType(ValueType):
    def validate(cls, v: typing.Any) -> typing.Any:

        if not isinstance(v, networkx.Graph):
            raise ValueError(f"Invalid type '{type(v)}' for graph: {v}")
        return v

    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        graph: nx.Graph = v
        return {
            "python_cls": f"{v.__class__.__module__}.{v.__class__.__name__}",
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
