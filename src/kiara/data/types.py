# -*- coding: utf-8 -*-
import typing
from abc import ABCMeta
from enum import Enum
from faker import Faker

fake = Faker()


class ValueType(Enum):
    """Supported value types.

    It's very early days, so this does not really do anything yet.
    """

    def __new__(cls, *args, **kwds):
        value = args[0]["id"]
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    def __init__(self, type_map: typing.Mapping[str, typing.Any]):

        for k, v in type_map.items():
            setattr(self, k, v)

    any = {"id": "any", "python": object, "fake_value": fake.pydict}
    integer = {"id": "integer", "python": int, "fake_value": fake.pyint}
    string = {"id": "string", "python": str, "fake_value": fake.pystr}
    dict = {"id": "dict", "python": dict, "fake_value": fake.pydict}
    boolean = {"id": "boolean", "python": bool, "fake_value": fake.pybool}
    table = {
        "id": "table",
        "python": typing.List[typing.Dict],
        "fake_value": fake.pydict,
    }
    value_items = {
        "id": "value_items",
        "python": dict,
        "fake_value": NotImplemented,
    }


class KiaraValueType(metaclass=ABCMeta):
    def __init__(self):

        pass

    def serialize(self, object: typing.Any):
        pass

    def deserialize(self, object: typing.Any):
        pass
