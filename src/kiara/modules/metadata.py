# -*- coding: utf-8 -*-
import abc
import typing
import uuid
from pydantic import BaseModel, Field

from kiara.data import Value, ValueSet
from kiara.data.values import ValueSchema
from kiara.defaults import NO_HASH_MARKER
from kiara.exceptions import KiaraProcessingException
from kiara.metadata.core_models import HashMetadata, PythonClassMetadata
from kiara.module import KiaraModule
from kiara.module_config import KiaraModuleConfig


class MetadataModuleConfig(KiaraModuleConfig):

    value_type: str = Field(description="The data type this module will be used for.")


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
        data_type = self.get_config_value("value_type")
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
            "metadata_item": {
                "type": "dict",
                "doc": "The metadata for the provided value.",
            },
            "metadata_item_schema": {
                "type": "string",
                "doc": "The (json) schema for the metadata.",
            },
        }

        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value = inputs.get_value_obj("value_item")
        if value.type_name not in [self.value_type, "any"]:
            raise KiaraProcessingException(
                f"Can't extract metadata for value of type '{value.value_schema.type}'. Expected type '{self.value_type}'."
            )

        # TODO: if type 'any', validate that the data is actually of the right type?

        outputs.set_value("metadata_item_schema", self.metadata_schema)
        metadata = self.extract_metadata(value)
        if isinstance(metadata, BaseModel):
            metadata = metadata.dict()

        # TODO: validate metadata?
        outputs.set_value("metadata_item", metadata)


class ExtractPythonClass(ExtractMetadataModule):
    """Extract metadata about the Python type of this value."""

    _module_type_name = "metadata.python_class"

    @classmethod
    def _get_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "*"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "python_cls"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:

        return PythonClassMetadata

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        item = value.get_value_data()
        cls = item.__class__

        return {
            "class_name": cls.__name__,
            "module_name": cls.__module__,
            "full_name": f"{cls.__module__}.{cls.__name__}",
        }


class CalculateValueHashModule(ExtractMetadataModule):
    """Calculate the hash of a value."""

    _module_type_name = "metadata.value_hash"

    @classmethod
    def _get_supported_types(self) -> typing.Union[str, typing.Iterable[str]]:
        return "*"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "value_hash"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:

        return HashMetadata

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        value_hash = value.calculate_hash()
        hash_desc = f"Hash for value type {value.type_name}."

        if value_hash == NO_HASH_MARKER:
            value_hash = str(uuid.uuid4())
            hash_desc = "Generated uuid."

        return {"hash": value_hash, "hash_desc": hash_desc}
