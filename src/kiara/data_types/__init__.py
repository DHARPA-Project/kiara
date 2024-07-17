# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""
This is the base module that contains everything data type-related in *kiara*.

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
import uuid
from typing import TYPE_CHECKING, Any, Generic, Mapping, Tuple, Type, TypeVar, Union

import structlog
from deepdiff import DeepHash
from pydantic import BaseModel, ConfigDict, PrivateAttr, ValidationError
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table

from kiara.defaults import (
    INVALID_HASH_MARKER,
    INVALID_SIZE_MARKER,
    NO_SERIALIZATION_MARKER,
    SpecialValue,
)
from kiara.exceptions import KiaraValueException, ValueTypeConfigException
from kiara.models.python_class import PythonClass
from kiara.models.values import DataTypeCharacteristics, ValueStatus
from kiara.models.values.value_schema import ValueSchema
from kiara.utils.hashing import KIARA_HASH_FUNCTION

#
#     if obj.__class__.__module__ == "builtins":
#         return obj.__class__.__name__
#     else:
#         return f"{obj.__class__.__module__}.{obj.__class__.__name__}"

if TYPE_CHECKING:
    from kiara.models.values.value import (
        DataTypeInfo,
        SerializedData,
        Value,
        ValuePedigree,
    )

logger = structlog.getLogger()

# def get_type_name(obj: Any):
#     """Utility function to get a pretty string from the class of an object."""


class DataTypeConfig(BaseModel):
    """
    Base class that describes the configuration a [``DataType``][kiara.data.data_types.DataType] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``DataType`` class. By default,
    a ``DataType`` is not configurable, unless the ``_config_cls`` class attribute points to a sub-class of this class.
    """

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def requires_config(cls) -> bool:
        """Return whether this class can be used as-is, or requires configuration before an instance can be created."""
        for field_name, field in cls.model_fields.items():
            if field.is_required() and field.default is None:
                return True
        return False

    _config_hash: Union[int, None] = PrivateAttr(default=None)

    def get(self, key: str) -> Any:
        """Get the value for the specified configuation key."""
        if key not in self.model_fields.keys():
            raise Exception(
                f"No config value '{key}' in module config class '{self.__class__.__name__}'."
            )

        return getattr(self, key)

    @property
    def config_hash(self) -> int:

        if self._config_hash is None:
            _d = self.model_dump()
            hashes = DeepHash(_d)
            self._config_hash = hashes[_d]
        return self._config_hash

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False

        return self.model_dump() == other.model_dump()

    def __hash__(self):

        return self.config_hash

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        my_table = Table(box=box.MINIMAL, show_header=False)
        my_table.add_column("Field name", style="i")
        my_table.add_column("Value")
        for field in self.model_fields.keys():
            my_table.add_row(field, getattr(self, field))

        yield my_table


TYPE_PYTHON_CLS = TypeVar("TYPE_PYTHON_CLS")
TYPE_CONFIG_CLS = TypeVar("TYPE_CONFIG_CLS", bound=DataTypeConfig)


class DataType(abc.ABC, Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS]):
    """
    Base class that all *kiara* data_types must inherit from.

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
    def retrieve_available_type_profiles(cls) -> Mapping[str, Mapping[str, Any]]:
        return {}

    @classmethod
    @abc.abstractmethod
    def python_class(cls) -> Type[TYPE_PYTHON_CLS]:
        """The Python class that the internal 'data' attribute of a value has."""

    @classmethod
    def data_type_config_class(cls) -> Type[TYPE_CONFIG_CLS]:
        """The Python class that holds the (optional) configuration for a data type instance."""
        return DataTypeConfig  # type: ignore

    @classmethod
    def _calculate_data_type_hash(
        cls, data_type_config: Union[Mapping[str, Any], DataTypeConfig]
    ) -> int:

        if isinstance(data_type_config, Mapping):
            data_type_config = cls.data_type_config_class()(**data_type_config)  # type: ignore

        obj = {
            "type": cls._data_type_name,  # type: ignore
            "type_config": data_type_config.config_hash,
        }
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        result: int = h[obj]
        return result

    def __init__(self, **type_config: Any):

        try:
            self._type_config: TYPE_CONFIG_CLS = (
                self.__class__.data_type_config_class()(**type_config)
            )
        except ValidationError as ve:
            raise ValueTypeConfigException(
                f"Error creating object for type: {ve}",
                self.__class__,
                type_config,
                ve,
            )

        self._data_type_hash: Union[int, None] = None
        self._characteristics: Union[DataTypeCharacteristics, None] = None
        self._info: Union[DataTypeInfo, None] = None

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
    def info(self) -> "DataTypeInfo":

        if self._info is not None:
            return self._info

        from kiara.models.values.value import DataTypeInfo

        self._info = DataTypeInfo(
            data_type_name=self.data_type_name,
            data_type_config=self.type_config.model_dump(),
            characteristics=self.characteristics,
            data_type_class=PythonClass.from_class(self.__class__),
        )
        self._info._data_type_instance = self
        return self._info

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

    def calculate_hash(self, data: "SerializedData") -> str:
        """Calculate the hash of the value."""
        return data.instance_id

    def calculate_size(self, data: "SerializedData") -> int:
        """Calculate the size of the value."""
        return data.data_size

    def serialize_as_json(self, data: Any) -> "SerializedData":

        _data = {"data": {"type": "inline-json", "inline_data": data, "codec": "json"}}

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.model_dump(),
            "data": _data,
            "serialization_profile": "json",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "deserialize.from_json",
                        "module_config": {"result_path": "data"},
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def serialize(self, data: TYPE_PYTHON_CLS) -> Union[None, str, "SerializedData"]:

        logger.debug(
            "ignore.serialize_request",
            data_type=self.data_type_name,
            reason="no 'serialize' method imnplemented",
        )
        return NO_SERIALIZATION_MARKER
        # raise NotImplementedError(f"Data type '{self.data_type_name}' does not support serialization.")
        #
        # try:
        #     import pickle5 as pickle
        # except Exception:
        #     import pickle  # type: ignore
        #
        # pickled = pickle.dumps(data, protocol=5)
        # _data = {"python_object": {"type": "chunk", "chunk": pickled, "codec": "raw"}}
        #
        # serialized_data = {
        #     "data_type": self.data_type_name,
        #     "data_type_config": self.type_config.dict(),
        #     "data": _data,
        #     "serialization_profile": "pickle",
        #     "serialization_metadata": {
        #         "profile": "pickle",
        #         "environment": {},
        #         "deserialize": {
        #             "object": {
        #                 "module_name": "value.unpickle",
        #                 "module_config": {
        #                     "value_type": "any",
        #                     "target_profile": "object",
        #                     "serialization_profile": "pickle",
        #                 },
        #             }
        #         },
        #     },
        # }
        # from kiara.models.values.value import SerializationResult
        #
        # serialized = SerializationResult(**serialized_data)
        # return serialized

    @property
    def type_config(self) -> TYPE_CONFIG_CLS:
        return self._type_config

    def _pre_examine_data(
        self, data: Any, schema: ValueSchema
    ) -> Tuple[Any, Union[str, "SerializedData"], ValueStatus, str, int]:

        assert data is not None

        if data is SpecialValue.NOT_SET:
            status = ValueStatus.NOT_SET
            data = None
        elif data is SpecialValue.NO_VALUE:
            status = ValueStatus.NONE
            data = None
        else:
            status = ValueStatus.SET

        # if data is None and schema.default not in [
        #     None,
        #     SpecialValue.NO_VALUE,
        #     SpecialValue.NOT_SET,
        # ]:
        #
        #     status = ValueStatus.DEFAULT
        #     if callable(schema.default):
        #         data = schema.default()
        #     else:
        #         data = copy.deepcopy(schema.default)

        if data is None or data is SpecialValue.NOT_SET:
            # if schema.default in [None, SpecialValue.NO_VALUE]:
            #     data = SpecialValue.NO_VALUE
            #     status = ValueStatus.NONE
            # elif schema.default == SpecialValue.NOT_SET:
            #     data = SpecialValue.NOT_SET
            #     status = ValueStatus.NOT_SET

            size = 0
            value_hash = INVALID_HASH_MARKER
            serialized: Union[None, str, "SerializedData"] = NO_SERIALIZATION_MARKER
        else:

            from kiara.models.values.value import SerializedData

            if isinstance(data, SerializedData):
                # TODO: assert value is in schema lineage
                # assert data.data_type == schema.type
                # assert data.data_type_config == schema.type_config
                serialized = data
                not_serialized: bool = False
            else:

                data = self.parse_python_obj(data)

                if data is None:
                    raise Exception(
                        f"Invalid data, can't parse into a value of type '{schema.type}'."
                    )
                self._validate(data)

                serialized = self.serialize(data)

                if serialized is None:
                    serialized = NO_SERIALIZATION_MARKER

                if isinstance(serialized, str):
                    not_serialized = True
                else:
                    not_serialized = False

            if not_serialized:
                size = INVALID_SIZE_MARKER
                value_hash = INVALID_HASH_MARKER
            else:
                size = serialized.data_size  # type: ignore
                value_hash = serialized.instance_id  # type: ignore

        assert serialized is not None
        result = (data, serialized, status, value_hash, size)
        return result

    def assemble_value(
        self,
        value_id: uuid.UUID,
        data: Any,
        schema: ValueSchema,
        environment_hashes: Mapping[str, Mapping[str, str]],
        serialized: Union[str, "SerializedData"],
        status: Union[ValueStatus, str],
        value_hash: str,
        value_size: int,
        pedigree: "ValuePedigree",
        kiara_id: uuid.UUID,
        pedigree_output_name: str,
    ) -> Tuple["Value", Any]:

        from kiara.models.values.value import Value

        if isinstance(status, str):
            status = ValueStatus(status).name

        if status in [ValueStatus.SET, ValueStatus.DEFAULT]:
            try:

                value = Value(
                    value_id=value_id,
                    kiara_id=kiara_id,
                    value_status=status,
                    value_size=value_size,
                    value_hash=value_hash,
                    value_schema=schema,
                    environment_hashes=environment_hashes,
                    pedigree=pedigree,
                    pedigree_output_name=pedigree_output_name,
                    data_type_info=self.info,
                )

            except Exception as e:
                raise KiaraValueException(
                    data_type=self.__class__, value_data=data, parent=e
                )
        else:
            value = Value(
                value_id=value_id,
                kiara_id=kiara_id,
                value_status=status,
                value_size=value_size,
                value_hash=value_hash,
                value_schema=schema,
                environment_hashes=environment_hashes,
                pedigree=pedigree,
                pedigree_output_name=pedigree_output_name,
                data_type_info=self.info,
            )

        value._value_data = data
        value._serialized_data = serialized
        return value, data

    def parse_python_obj(self, data: Any) -> TYPE_PYTHON_CLS:
        """
        Parse a value into a supported python type.

        This exists to make it easier to do trivial conversions (e.g. from a date string to a datetime object).
        If you choose to overwrite this method, make 100% sure that you don't change the meaning of the value, and try to
        avoid adding or removing information from the data (e.g. by changing the resolution of a date).

        Arguments:
        ---------
            v: the value

        Returns:
        -------
            'None', if no parsing was done and the original value should be used, otherwise return the parsed Python object
        """

        # this would in most cases be overwritten by an implementing class
        # if not, then the _validate method should catch the issue
        return data  # type: ignore

    def _validate(self, value: TYPE_PYTHON_CLS) -> None:
        """Validate the value. This expects an instance of the defined Python class (from 'backing_python_type)."""
        if not isinstance(value, self.__class__.python_class()):
            raise ValueError(
                f"Invalid python type '{type(value)}', must be: {self.__class__.python_class()}"
            )

    def create_renderable(self, **config):

        show_type_info = config.get("show_type_info", False)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key")
        table.add_column("value", style="i")
        table.add_row("type_name", self.data_type_name)
        config_json = self.type_config.model_dump_json(exclude_unset=True, indent=2)
        config = Syntax(config_json, "json", background_color="default")
        table.add_row("type_config", config)

        if show_type_info:
            from kiara.interfaces.python_api.models.info import DataTypeClassInfo

            info = DataTypeClassInfo.create_from_type_class(self.__class__)
            table.add_row("", "")
            table.add_row("", Rule())
            table.add_row("type_info", info)

        return table


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
