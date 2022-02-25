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

Anyway, the way it works (for now) is that *kiara* comes with a set of often used types (the standard set of: scalars,
list, dict, table & array, etc.) which each come with 2 functions that can serialize and deserialize values of that
type in a persistant fashion -- which could be storing as a file on disk, or as a cell/row in a database. Those functions
will most likley be *kiara* modules themselves, with even more restricted input/output type options.

In addition, packages that contain modules can implement their own, custom types, if suitable ones are not available in
core-*kiara*. Those can either be 'serialized/deserialized' into *kiara*-native types (which in turn will serialize them
using their own serializing functions), or will have to implement custom serializing functionality (which will probably
be discouraged, since this might not be trivial and there are quite a few things to consider).

"""
import abc
import deepdiff
import typing
from pydantic import BaseModel, Extra, Field, PrivateAttr, ValidationError
from pydantic.schema import (
    get_flat_models_from_model,
    get_model_name_map,
    model_process_schema,
)
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.exceptions import KiaraValueException, ValueTypeConfigException
from kiara.metadata import ValueTypeAndDescription, WrapperMetadataModel
from kiara.metadata.core_models import PythonClassMetadata
from kiara.metadata.type_models import ValueTypeMetadata


def get_type_name(obj: typing.Any):
    """Utility function to get a pretty string from the class of an object."""

    if obj.__class__.__module__ == "builtins":
        return obj.__class__.__name__
    else:
        return f"{obj.__class__.__module__}.{obj.__class__.__name__}"


class ValueTypeConfigSchema(BaseModel):
    """Base class that describes the configuration a [``ValueType``][kiara.data.types.ValueType] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``ValueType`` class. By default,
    a ``ValueType`` is not configurable, unless the ``_config_cls`` class attribute points to a sub-class of this class.
    """

    @classmethod
    def requires_config(cls) -> bool:
        """Return whether this class can be used as-is, or requires configuration before an instance can be created."""

        for field_name, field in cls.__fields__.items():
            if field.required and field.default is None:
                return True
        return False

    _config_hash: str = PrivateAttr(default=None)

    class Config:
        extra = Extra.forbid
        allow_mutation = False

    def get(self, key: str) -> typing.Any:
        """Get the value for the specified configuation key."""

        if key not in self.__fields__:
            raise Exception(
                f"No config value '{key}' in module config class '{self.__class__.__name__}'."
            )

        return getattr(self, key)

    @property
    def config_hash(self):

        if self._config_hash is None:
            _d = self.dict()
            hashes = deepdiff.DeepHash(_d)
            self._config_hash = hashes[_d]
        return self._config_hash

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False

        return self.dict() == other.dict()

    def __hash__(self):

        return hash(self.config_hash)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        my_table = Table(box=box.MINIMAL, show_header=False)
        my_table.add_column("Field name", style="i")
        my_table.add_column("Value")
        for field in self.__fields__:
            my_table.add_row(field, getattr(self, field))

        yield my_table


TYPE_CONFIG_CLS = typing.TypeVar("TYPE_CONFIG_CLS", bound=ValueTypeConfigSchema)
TYPE_PYTHON_CLS = typing.TypeVar("TYPE_PYTHON_CLS")


class ValueTypeConfigMetadata(WrapperMetadataModel):
    @classmethod
    def from_config_class(
        cls,
        config_cls: typing.Type[ValueTypeConfigSchema],
    ):

        flat_models = get_flat_models_from_model(config_cls)
        model_name_map = get_model_name_map(flat_models)
        m_schema, _, _ = model_process_schema(config_cls, model_name_map=model_name_map)
        fields = m_schema["properties"]

        config_values = {}
        for field_name, details in fields.items():
            type_str = "-- n/a --"
            if "type" in details.keys():
                type_str = details["type"]

            desc = details.get("description", DEFAULT_NO_DESC_VALUE)

            default = config_cls.__fields__[field_name].default

            if default is None:
                if callable(config_cls.__fields__[field_name].default_factory):
                    default = config_cls.__fields__[field_name].default_factory()  # type: ignore

            req = config_cls.__fields__[field_name].required

            config_values[field_name] = ValueTypeAndDescription(
                description=desc, type=type_str, value_default=default, required=req
            )

        python_cls = PythonClassMetadata.from_class(config_cls)
        return ValueTypeConfigMetadata(
            python_class=python_cls, config_values=config_values
        )

    config_values: typing.Dict[str, ValueTypeAndDescription] = Field(
        description="The available configuration values."
    )


class ValueType(abc.ABC, typing.Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS]):
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
    def get_type_metadata(cls) -> ValueTypeMetadata:
        return ValueTypeMetadata.from_value_type_class(cls)

    # @classmethod
    # def doc(cls) -> str:
    #
    #     return extract_doc_from_cls(cls)
    #
    # @classmethod
    # def desc(cls) -> str:
    #     return extract_doc_from_cls(cls, only_first_line=True)

    # @classmethod
    # def conversions(
    #     self,
    # ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
    #     """Return a dictionary of configuration for modules that can transform this type.
    #
    #     The name of the transformation is the key of the result dictionary, the configuration is a module configuration
    #     (dictionary wth 'module_type' and optional 'module_config', 'input_name' and 'output_name' keys).
    #     """
    #
    #     return {"string": {"module_type": "string.pretty_print", "input_name": "item"}}

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:
        """Check whether the provided input matches this value type.

        If it does, return a ValueType object (with the appropriate type configuration).
        """
        return None

    @classmethod
    @abc.abstractmethod
    def backing_python_type(cls) -> typing.Type[TYPE_PYTHON_CLS]:
        pass

    @classmethod
    @abc.abstractmethod
    def type_config_cls(cls) -> typing.Type[TYPE_CONFIG_CLS]:
        pass

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return None

    @classmethod
    def get_supported_hash_types(cls) -> typing.Iterable[str]:
        return []

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:
        """Calculate the hash of this value.

        If a hash can't be calculated, or the calculation of a type is not implemented (yet), this will return None.
        """

        raise Exception(f"Value type '{cls._value_type_name}' does not support hash calculation.")  # type: ignore

    def __init__(self, **type_config: typing.Any):

        try:
            self._type_config: TYPE_CONFIG_CLS = self.__class__.type_config_cls(**type_config)  # type: ignore  # TODO: double-check this is only a mypy issue
        except ValidationError as ve:
            raise ValueTypeConfigException(
                f"Error creating object for value_type: {ve}",
                self.__class__,
                type_config,
                ve,
            )
        # self._type_config: typing.Mapping[str, typing.Any] = self
        # self._transformations: typing.Optional[
        #     typing.Mapping[str, typing.Mapping[str, typing.Any]]
        # ] = None

    @property
    def type_config(self) -> TYPE_CONFIG_CLS:
        return self._type_config

    def import_value(self, value: typing.Any) -> typing.Any:

        assert value is not None

        try:
            parsed = self.parse_value(value)
            if parsed is None:
                parsed = value
            self.validate(parsed)
        except Exception as e:
            raise KiaraValueException(
                value_type=self.__class__, value_data=value, exception=e
            )

        return parsed

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
        """Validate the value. This expects an instance of the defined Python class (from 'backing_python_type)."""

    def get_type_hint(self, context: str = "python") -> typing.Optional[typing.Type]:
        """Return a type hint for this value type object.

        This can be used by kiara interfaces to document/validate user input. For now, only 'python' type hints are
        expected to be implemented, but in theory this could also return type hints for other contexts.
        """
        return None

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        raise NotImplementedError()


# class ValueTypeInfo(object):
#     def __init__(self, value_type_cls: typing.Type[ValueType]):
#
#         self._value_type_cls: typing.Type[ValueType] = value_type_cls
#
#     @property
#     def doc(self) -> str:
#         return self._value_type_cls.doc()
#
#     @property
#     def desc(self) -> str:
#         return self._value_type_cls.desc()


class ValueTypesInfo(object):
    def __init__(
        self,
        value_type_classes: typing.Mapping[str, typing.Type[ValueType]],
        details: bool = False,
    ):

        self._value_type_classes: typing.Mapping[
            str, typing.Type[ValueType]
        ] = value_type_classes
        self._details: bool = details

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        table = Table(show_header=True, box=box.SIMPLE, show_lines=False)
        table.add_column("Type name", style="i")
        table.add_column("Description")

        for type_name in sorted(self._value_type_classes.keys()):
            t_md = self._value_type_classes[type_name].get_type_metadata()
            if self._details:
                md = Markdown(t_md.documentation.full_doc)
            else:
                md = Markdown(t_md.documentation.description)
            table.add_row(type_name, md)

        panel = Panel(table, title="Available value types", title_align="left")
        yield panel
