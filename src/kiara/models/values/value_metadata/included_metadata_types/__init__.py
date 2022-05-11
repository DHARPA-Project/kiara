# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import TYPE_CHECKING, Iterable

from kiara.models.filesystem import FileBundle, FileModel
from kiara.models.python_class import PythonClass
from kiara.models.values.value_metadata import ValueMetadata

if TYPE_CHECKING:
    from kiara.models.values.value import Value


class PythonClassMetadata(ValueMetadata):
    """Python class and module information."""

    _metadata_key = "python_class"
    _kiara_model_id = "metadata.python_class"

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
    _kiara_model_id = "metadata.file"

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["file"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "FileMetadata":

        return FileMetadata.construct(file=value.data)

    file: FileModel = Field(description="The file-specific metadata.")


class FileBundleMetadata(ValueMetadata):
    """File bundle stats."""

    _metadata_key = "file_bundle"
    _kiara_model_id = "metadata.file_bundle"

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["file_bundle"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "FileBundleMetadata":

        return FileBundleMetadata.construct(file_bundle=value.data)

    file_bundle: FileBundle = Field(description="The file-specific metadata.")
