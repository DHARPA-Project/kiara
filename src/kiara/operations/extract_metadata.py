# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import BaseModel, Field

from kiara import Kiara, KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfig
from kiara.operations import Operation, OperationType
from kiara.utils import log_message


class MetadataModuleConfig(ModuleTypeConfig):

    value_type: str = Field(description="The data type this module will be used for.")


class ExtractMetadataModule(KiaraModule):
    """Base class to use when writing a module to extract metadata from a file.

    It's possible to use any arbitrary *kiara* module for this purpose, but sub-classing this makes it easier.
    """

    _config_cls = MetadataModuleConfig

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        value_types: typing.Iterable = cls.get_supported_value_types()
        if "*" in value_types:
            value_types = kiara.type_mgmt.value_type_names

        metadata_key = cls.get_metadata_key()
        for value_type in value_types:

            if value_type not in kiara.type_mgmt.value_type_names:
                log_message(
                    f"Ignoring metadata-extract operation (metadata key: {metadata_key}) for type '{value_type}': type not available"
                )
                continue

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": {"value_type": value_type},
                "doc": f"Extract '{metadata_key}' for value of type '{value_type}'.",
            }

            all_metadata_profiles[
                f"{value_type}.extract_metadata.{metadata_key}"
            ] = op_config

        return all_metadata_profiles

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
        if self.value_type != "any" and value.type_name != self.value_type:
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


class ExtractMetadataOperationType(OperationType):
    """Extract metadata from a dataset."""

    def is_matching_operation(self, op_config: Operation) -> bool:
        return issubclass(op_config.module_cls, ExtractMetadataModule)

    def get_all_operations_for_type(
        self, value_type: str
    ) -> typing.Mapping[str, Operation]:

        result = {}
        for op_config in self.operation_configs.values():
            if op_config.module_config["value_type"] != value_type:
                continue
            module_cls: ExtractMetadataModule = op_config.module_cls  # type: ignore
            md_key = module_cls.get_metadata_key()
            result[md_key] = op_config
        return result
