# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson.orjson
import structlog
from pydantic import Field
from rich.console import Group
from typing import Any, Mapping, Optional, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType, KiaraModelValueType
from kiara.defaults import KIARA_HASH_FUNCTION
from kiara.models.filesystem import FileBundle, FileModel
from kiara.models.values.value import Value

logger = structlog.getLogger()


class FileTypeConfig(DataTypeConfig):

    content_type: Optional[str] = Field(
        description="The content type of this file.", default=None
    )


SUPPORTED_FILE_TYPES = ["csv", "json"]


class FileValueType(KiaraModelValueType[FileModel, FileTypeConfig]):
    """A file."""

    _data_type_name = "file"

    @classmethod
    def retrieve_available_type_profiles(cls) -> Mapping[str, Mapping[str, Any]]:
        result = {}
        for ft in SUPPORTED_FILE_TYPES:
            result[f"{ft}_file"] = {"content_type": ft}
        return result

    @classmethod
    def python_class(cls) -> Type:
        return FileModel

    @classmethod
    def data_type_config_class(cls) -> Type[FileTypeConfig]:
        return FileTypeConfig

    @classmethod
    def value_class(cls) -> Type[Value]:
        return Value

    def calculate_size(self, data: FileModel) -> int:
        return data.size

    def calculate_hash(self, data: FileModel) -> int:
        return KIARA_HASH_FUNCTION(data.file_hash)

    def create_model_from_python_obj(self, data: Any) -> FileModel:

        if isinstance(data, Mapping):
            return FileModel(**data)
        if isinstance(data, str):
            return FileModel.load_file(source=data)
        else:
            raise Exception(f"Can't create FileModel from data of type '{type(data)}'.")

    def render_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: Any = value.data
        max_lines = render_config.get("max_lines", 34)
        try:
            lines = []
            with open(data.path, "r") as f:
                for idx, l in enumerate(f):
                    if idx > max_lines:
                        lines.append("...\n")
                        lines.append("...")
                        break
                    lines.append(l)

            # TODO: syntax highlighting
            return "\n".join(lines)
        except UnicodeDecodeError:
            # found non-text data
            lines = [
                "Binary file or non-utf8 enconding, not printing content...",
                "",
                "[b]File metadata:[/b]",
                "",
                data.json(option=orjson.OPT_INDENT_2),
            ]
            return "\n".join("lines")

    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: Any = value.data
        max_lines = render_config.get("max_lines", 34)
        try:
            lines = []
            with open(data.path, "r") as f:
                for idx, l in enumerate(f):
                    if idx > max_lines:
                        lines.append("...\n")
                        lines.append("...")
                        break
                    lines.append(l.rstrip())

            return Group(*lines)
        except UnicodeDecodeError:
            # found non-text data
            lines = [
                "Binary file or non-utf8 enconding, not printing content...",
                "",
                "[b]File metadata:[/b]",
                "",
                data.json(option=orjson.OPT_INDENT_2),
            ]
            return Group(*lines)


class FileBundleValueType(AnyType[FileBundle, FileTypeConfig]):
    """A bundle of files (like a folder, zip archive, etc.)."""

    _data_type_name = "file_bundle"

    @classmethod
    def retrieve_available_type_profiles(cls) -> Mapping[str, Mapping[str, Any]]:
        result = {}
        for ft in SUPPORTED_FILE_TYPES:
            result[f"{ft}_file_bundle"] = {"content_type": ft}
        return result

    @classmethod
    def python_class(cls) -> Type:
        return FileBundle

    @classmethod
    def data_type_config_class(cls) -> Type[FileTypeConfig]:
        return FileTypeConfig

    @classmethod
    def value_class(cls) -> Type[Value]:
        return Value

    def calculate_size(self, data: FileBundle) -> int:
        return data.size

    def calculate_hash(self, data: FileBundle) -> int:
        return data.file_bundle_hash

    def create_model_from_python_obj(self, data: Any) -> FileBundle:

        if isinstance(data, str):
            return FileBundle.import_folder(source=data)
        else:
            raise Exception(
                f"Can't create FileBundle from data of type '{type(data)}'."
            )

    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        bundle: FileBundle = value.data
        renderable = bundle.create_renderable(**render_config)
        return renderable
