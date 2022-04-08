# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""This is the base module that contains everything data type-related in *kiara*.

I'm still not 100% sure how to best implement the *kiara* type system, there are several ways it could be done, for example
based on Python type-hints, using JSON-schema, Avro (which is my 2nd favourite option), as well as by implementing a
custom type-class hierarchy. Which is what I have choosen to try first. For now, it looks like it'll work out,
but there is a chance requirements I haven't forseen will crop up that could make this become ugly.

Anyway, the way it works (for now) is that *kiara* comes with a set of often used data_types (the standard set of: scalars,
list, dict, table & array, etc.) which each come with 2 functions that can serialize and deserialize values of that
type in a persistant fashion -- which could be storing as a file on disk, or as a cell/row in a database. Those functions
will most likley be *kiara* modules themselves, with even more restricted input/output type options.

In addition, packages that contain modules can implement their own, custom data_types, if suitable ones are not available in
core-*kiara*. Those can either be 'serialized/deserialized' into *kiara*-native data_types (which in turn will serialize them
using their own serializing functions), or will have to implement custom serializing functionality (which will probably
be discouraged, since this might not be trivial and there are quite a few things to consider).

"""
import abc
import copy
import uuid
from deepdiff import DeepHash
from pydantic import BaseModel, Extra, Field, PrivateAttr, ValidationError
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.table import Table
from typing import Any, Generic, Mapping, Optional, Tuple, Type, TypeVar, Union

from kiara.defaults import KIARA_HASH_FUNCTION, SpecialValue
from kiara.exceptions import KiaraValueException, ValueTypeConfigException
from kiara.models.python_class import PythonClass
from kiara.models.values import ValueStatus
from kiara.models.values.value import Value, ValuePedigree
from kiara.models.values.value_schema import ValueSchema


def get_type_name(obj: Any):
    """Utility function to get a pretty string from the class of an object."""

    if obj.__class__.__module__ == "builtins":
        return obj.__class__.__name__
    else:
        return f"{obj.__class__.__module__}.{obj.__class__.__name__}"


class DataTypeConfig(BaseModel):
    """Base class that describes the configuration a [``DataType``][kiara.data.data_types.DataType] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``DataType`` class. By default,
    a ``DataType`` is not configurable, unless the ``_config_cls`` class attribute points to a sub-class of this class.
    """

    @classmethod
    def requires_config(cls) -> bool:
        """Return whether this class can be used as-is, or requires configuration before an instance can be created."""

        for field_name, field in cls.__fields__.items():
            if field.required and field.default is None:
                return True
        return False

    _config_hash: Optional[int] = PrivateAttr(default=None)

    class Config:
        extra = Extra.forbid
        # allow_mutation = False

    def get(self, key: str) -> Any:
        """Get the value for the specified configuation key."""

        if key not in self.__fields__:
            raise Exception(
                f"No config value '{key}' in module config class '{self.__class__.__name__}'."
            )

        return getattr(self, key)

    @property
    def config_hash(self) -> int:

        if self._config_hash is None:
            _d = self.dict()
            hashes = DeepHash(_d)
            self._config_hash = hashes[_d]
        return self._config_hash

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False

        return self.dict() == other.dict()

    def __hash__(self):

        return self.config_hash

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        my_table = Table(box=box.MINIMAL, show_header=False)
        my_table.add_column("Field name", style="i")
        my_table.add_column("Value")
        for field in self.__fields__:
            my_table.add_row(field, getattr(self, field))

        yield my_table


TYPE_PYTHON_CLS = TypeVar("TYPE_PYTHON_CLS")
TYPE_CONFIG_CLS = TypeVar("TYPE_CONFIG_CLS", bound=DataTypeConfig)


class DataTypeCharacteristics(BaseModel):

    is_skalar: bool = Field(
        description="Whether the data desribed by this data type behaves like a skalar.",
        default=False,
    )
    is_json_serializable: bool = Field(
        description="Whether the data can be serialized to json without information loss.",
        default=False,
    )


class DataType(abc.ABC, Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS]):
    """Base class that all *kiara* data_types must inherit from.

    *kiara* data_types have 3 main responsibilities:

     - serialize into / deserialize from persistent state
     - data validation
     - metadata extraction

     Serializing being the arguably most important of those, because without most of the data management features of
     *kiara* would be impossible. Validation should not require any explanation. Metadata extraction is important, because
     that metadata will be available to other components of *kiara* (or frontends for it), without them having to request
     the actual data. That will hopefully make *kiara* very efficient in terms of memory management, as well as data
     transfer and I/O. Ideally, the actual data (bytes) will only be requested at the last possible moment. For example when a
     module needs the input data to do processing on it -- and even then it might be that it only requests a part of the
     data, say a single column of a table. Or when a frontend needs to display/visualize the data.
    """

    @classmethod
    @abc.abstractmethod
    def python_class(cls) -> Type[TYPE_PYTHON_CLS]:
        pass

    @classmethod
    def data_type_config_class(cls) -> Type[TYPE_CONFIG_CLS]:
        return DataTypeConfig  # type: ignore

    @classmethod
    def _calculate_data_type_hash(
        cls, data_type_config: Union[Mapping[str, Any], DataTypeConfig]
    ) -> int:

        if isinstance(data_type_config, Mapping):
            data_type_config = cls.data_type_config_class()(**data_type_config)

        obj = {
            "type": cls._data_type_name,  # type: ignore
            "type_config": data_type_config.config_hash,
        }
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        return h[obj]

    def __init__(self, **type_config: Any):

        try:
            self._type_config: TYPE_CONFIG_CLS = self.__class__.data_type_config_class()(**type_config)  # type: ignore  # TODO: double-check this is only a mypy issue
        except ValidationError as ve:
            raise ValueTypeConfigException(
                f"Error creating object for type: {ve}",
                self.__class__,
                type_config,
                ve,
            )

        self._data_type_hash: Optional[int] = None
        self._characteristics: Optional[DataTypeCharacteristics] = None

    @property
    def data_type_name(self) -> str:
        return self._data_type_name  # type: ignore

    @property
    def data_type_hash(self) -> int:
        if self._data_type_hash is None:
            self._data_type_hash = self.__class__._calculate_data_type_hash(
                self._type_config
            )
        return self._data_type_hash

    @property
    def characteristics(self) -> DataTypeCharacteristics:
        if self._characteristics is not None:
            return self._characteristics

        self._characteristics = self._retrieve_characteristics()
        return self._characteristics

    def _retrieve_characteristics(self) -> DataTypeCharacteristics:
        return DataTypeCharacteristics()

    # @abc.abstractmethod
    # def is_immutable(self) -> bool:
    #     pass

    @abc.abstractmethod
    def calculate_hash(self, data: TYPE_PYTHON_CLS) -> int:
        """Calculate the hash of the value."""

    @abc.abstractmethod
    def calculate_size(self, data: TYPE_PYTHON_CLS) -> int:
        pass

    @property
    def type_config(self) -> TYPE_CONFIG_CLS:
        return self._type_config

    def _pre_examine_data(
        self, data: Any, schema: ValueSchema
    ) -> Tuple[Any, ValueStatus, int]:

        if data == SpecialValue.NOT_SET:
            status = ValueStatus.NOT_SET
        else:
            status = ValueStatus.SET

            if data is None:
                if schema.default in [None, SpecialValue.NO_VALUE]:
                    data = SpecialValue.NO_VALUE
                    status = ValueStatus.NONE
                elif schema.default == SpecialValue.NOT_SET:
                    data = SpecialValue.NOT_SET
                    status = ValueStatus.NOT_SET
                elif callable(schema.default):
                    data = schema.default()
                    status = ValueStatus.DEFAULT
                else:
                    data = copy.deepcopy(schema.default)
                    status = ValueStatus.DEFAULT
            else:
                data = self.parse_python_obj(data)
                if data is None:
                    raise Exception(
                        f"Invalid data, can't parse into a value of type '{schema.type}'."
                    )

        if status in [ValueStatus.SET, ValueStatus.DEFAULT]:
            value_hash = self.calculate_hash(data)
        else:
            value_hash = 0

        return (data, status, value_hash)

    # def create_value(self, data: Any, schema: Optional[ValueSchema]=None, pedigree: Optional[ValuePedigree]=None) -> TYPE_VALUE_CLS:
    #
    #     if schema is None:
    #         raise NotImplementedError()
    #
    #     if pedigree is None:
    #         raise NotImplementedError()
    #
    #     data, status, value_hash = self._pre_examine_data(data=data, schema=schema)
    #
    #     v_id = uuid.uuid4()
    #     value = self.assemble_value(value_id=v_id, data=data, schema=schema, status=status, value_hash=value_hash, pedigree=pedigree, kiara_id=kiara_id)
    #     return value

    # def reassemble_value(self, value_id: uuid.UUID, load_config: "LoadConfig", schema: ValueSchema, status: Union[ValueStatus, str],
    #                        value_hash: int, value_size: int, pedigree: ValuePedigree, kiara_id: uuid.UUID,
    #                        pedigree_output_name: str) -> TYPE_VALUE_CLS:
    #
    #     if isinstance(status, str):
    #         status = ValueStatus(status)
    #
    #     value_cls = self.value_class()
    #     if status in [ValueStatus.SET, ValueStatus.DEFAULT]:
    #
    #         try:
    #
    #             value = value_cls(
    #                 value_id=value_id,
    #                 kiara_id=kiara_id,
    #                 value_status=status,
    #                 value_size=value_size,
    #                 value_hash=value_hash,
    #                 value_schema=schema,
    #                 pedigree=pedigree,
    #                 pedigree_output_name=pedigree_output_name
    #             )
    #
    #         except Exception as e:
    #             raise KiaraValueException(
    #                 data_type=self.__class__, value_data=load_config, exception=e
    #             )
    #         retriever = LoadConfigRetriever(load_config=load_config)
    #         value._data_retriever = retriever
    #
    #     else:
    #         assert value_size == 0
    #         value = value_cls(
    #             value_id=value_id,
    #             kiara_id=kiara_id,
    #             value_status=status,
    #             value_size=value_size,
    #             value_hash=value_hash,
    #             value_schema=schema,
    #             pedigree=pedigree,
    #             pedigree_output_name=pedigree_output_name
    #         )
    #
    #         if status == ValueStatus.NONE:
    #             data = SpecialValue.NO_VALUE
    #         else:
    #             data = SpecialValue.NOT_SET
    #
    #         retriever = StaticDataRetriever(data=data)
    #         value._data_retriever = retriever
    #
    #     return value

    def assemble_value(
        self,
        value_id: uuid.UUID,
        data: Any,
        schema: ValueSchema,
        status: Union[ValueStatus, str],
        value_hash: int,
        pedigree: ValuePedigree,
        kiara_id: uuid.UUID,
        pedigree_output_name: str,
    ) -> Tuple[Value, Any]:

        if isinstance(status, str):
            status = ValueStatus(status).name

        this_cls = PythonClass.from_class(self.__class__)
        if status in [ValueStatus.SET, ValueStatus.DEFAULT]:
            size = self.calculate_size(data)

            try:

                self._validate(data)

                value = Value(
                    value_id=value_id,
                    kiara_id=kiara_id,
                    value_status=status,
                    value_size=size,
                    value_hash=value_hash,
                    value_schema=schema,
                    pedigree=pedigree,
                    pedigree_output_name=pedigree_output_name,
                    data_type_class=this_cls,
                )

            except Exception as e:
                raise KiaraValueException(
                    data_type=self.__class__, value_data=data, exception=e
                )
        else:
            size = 0
            value = Value(
                value_id=value_id,
                kiara_id=kiara_id,
                value_status=status,
                value_size=size,
                value_hash=value_hash,
                value_schema=schema,
                pedigree=pedigree,
                pedigree_output_name=pedigree_output_name,
                data_type_class=this_cls,
            )

        value._value_data = data
        value._data_type = self
        return value, data

    def parse_python_obj(self, data: Any) -> TYPE_PYTHON_CLS:
        """Parse a value into a supported python type.

        This exists to make it easier to do trivial conversions (e.g. from a date string to a datetime object).
        If you choose to overwrite this method, make 100% sure that you don't change the meaning of the value, and try to
        avoid adding or removing information from the data (e.g. by changing the resolution of a date).

        Arguments:
            v: the value

        Returns:
            'None', if no parsing was done and the original value should be used, otherwise return the parsed Python object
        """

        return data

    def _validate(self, value: TYPE_PYTHON_CLS) -> None:
        """Validate the value. This expects an instance of the defined Python class (from 'backing_python_type)."""

        if not isinstance(value, self.__class__.python_class()):
            raise ValueError(
                f"Invalid python type '{type(value)}', must be: {self.__class__.python_class()}"
            )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        raise NotImplementedError()


# class ValueTypeInfo(object):
#     def __init__(self, type_cls: typing.Type[ValueTypeOrm]):
#
#         self._value_type_cls: typing.Type[ValueTypeOrm] = type_cls
#
#     @property
#     def doc(self) -> str:
#         return self._value_type_cls.doc()
#
#     @property
#     def desc(self) -> str:
#         return self._value_type_cls.desc()
