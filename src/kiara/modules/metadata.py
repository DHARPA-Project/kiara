# -*- coding: utf-8 -*-
import typing
from pydantic import BaseModel

from kiara.data import Value
from kiara.metadata.core_models import PythonClassMetadata
from kiara.operations.extract_metadata import ExtractMetadataModule


class ExtractPythonClass(ExtractMetadataModule):
    """Extract metadata about the Python type of a value."""

    _module_type_name = "metadata.python_class"

    @classmethod
    def _get_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "*"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "python_class"

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
