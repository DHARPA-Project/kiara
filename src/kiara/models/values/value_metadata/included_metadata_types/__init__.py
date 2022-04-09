# -*- coding: utf-8 -*-
from pydantic import Field
from typing import Iterable

from kiara.models.filesystem import FileModel
from kiara.models.python_class import PythonClass
from kiara.models.values.value_metadata import ValueMetadata


class PythonClassMetadata(ValueMetadata):
    """Python class and module information."""

    _metadata_key = "python_class"

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["any"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "PythonClassMetadata":

        return PythonClassMetadata.construct(
            python_class=PythonClass.from_class(value.data.__class__)
        )

    # metadata_key: Literal["python_class"]
    python_class: PythonClass = Field(
        description="Details about the Python class that backs this value."
    )


class FileMetadata(ValueMetadata):
    """File stats."""

    _metadata_key = "file"

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["file"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "FileMetadata":

        return FileMetadata.construct(file=value.data)

    file: FileModel = Field(description="The file-specific metadata.")
