# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import BaseModel, Field

from kiara import KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig


class MetadataModuleConfig(KiaraModuleConfig):

    type: str = Field(description="The data type this module will be used for.")


class ExtractMetadataModule(KiaraModule):
    """Base class to use when writing a module to extract metadata from a file.

    It's possible to use any arbitrary *kiara* module for this purpose, but sub-classing this makes it easier.
    """

    _config_cls = MetadataModuleConfig

    @classmethod
    @abc.abstractmethod
    def _get_supported_types(self) -> typing.Union[str, typing.Iterable[str]]:
        pass

    @classmethod
    def get_metadata_key(cls) -> str:
        return cls._module_type_name  # type: ignore

    @classmethod
    def get_supported_value_types(cls) -> typing.Set[str]:
        _types = cls._get_supported_types()
        if isinstance(_types, str):
            _types = [_types]

        return set(_types)

    def __init__(self, *args, **kwargs):

        self._metadata_schema: typing.Optional[str] = None
        super().__init__(*args, **kwargs)

    @property
    def value_type(self) -> str:
        data_type = self.get_config_value("type")
        sup_types = self.get_supported_value_types()
        if "*" not in sup_types and data_type not in sup_types:
            raise ValueError(
                f"Invalid module configuration, type '{data_type}' not supported. Supported types: {', '.join(self.get_supported_value_types())}."
            )

        return data_type

    @property
    def metadata_schema(self) -> str:
        if self._metadata_schema is not None:
            return self._metadata_schema

        schema = self._get_metadata_schema(type=self.value_type)
        if isinstance(schema, type) and issubclass(schema, BaseModel):
            schema = schema.schema_json()
        elif not isinstance(schema, str):
            raise TypeError(f"Invalid type for metadata schema: {type(schema)}")

        self._metadata_schema = schema
        return self._metadata_schema

    @abc.abstractmethod
    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        """Create the metadata schema for the configured type."""

    @abc.abstractmethod
    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:
        pass

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "value_item": {
                "type": self.value_type,
                "doc": f"A value of type '{self.value_type}'",
                "optional": False,
            }
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {
            "item_metadata": {
                "type": "dict",
                "doc": "The metadata for the provided value.",
            },
            "item_metadata_schema": {
                "type": "string",
                "doc": "The (json) schema for the metadata.",
            },
        }

        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value = inputs.get_value_obj("value_item")
        if value.value_schema.type != self.value_type:
            raise KiaraProcessingException(
                f"Can't extract metadata for value of type '{value.value_schema.type}'. Expected type '{self.value_type}'."
            )

        outputs.set_value("item_metadata_schema", self.metadata_schema)
        metadata = self.extract_metadata(value)
        # TODO: validate metadata?
        outputs.set_value("item_metadata", metadata)


class ExtractPythonClass(ExtractMetadataModule):
    """Extract metadata about the Python type of this value."""

    _module_type_name = "python_class"

    @classmethod
    def _get_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "*"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "python_cls"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        class PythonClassModel(BaseModel):
            class_name: str = Field(description="The name of the Python class")
            module_name: str = Field(
                description="The name of the Python module this class lives in."
            )
            full_name: str = Field(description="The full class namespace.")

        return PythonClassModel

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        item = value.get_value_data()
        cls = item.__class__

        return {
            "class_name": cls.__name__,
            "module_name": cls.__module__,
            "full_name": f"{cls.__module__}.{cls.__name__}",
        }
