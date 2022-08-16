# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson.orjson
import structlog
from pydantic import Field
from rich.console import Group
from typing import TYPE_CHECKING, Any, Dict, Mapping, Type, Union

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType, KiaraModelValueType
from kiara.models.filesystem import FileBundle, FileModel
from kiara.models.values.value import Value

if TYPE_CHECKING:
    from kiara.models.values.value import SerializedData

logger = structlog.getLogger()


class FileTypeConfig(DataTypeConfig):

    content_type: Union[str, None] = Field(
        description="The content type of this file.", default=None
    )


SUPPORTED_FILE_TYPES = ["csv", "json", "text", "binary"]


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

    def serialize(self, data: FileModel) -> "SerializedData":

        _data = {
            data.file_name: {
                "type": "file",
                "codec": "raw",
                "file": data.path,
            },
            "__file_metadata__": {
                "type": "inline-json",
                "codec": "json",
                "inline_data": {
                    "file_name": data.file_name,
                    # "import_time": data.import_time,
                },
            },
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": _data,
            "serialization_profile": "copy",
            "metadata": {
                # "profile": "",
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "deserialize.file",
                        "module_config": {
                            "value_type": "file",
                            "target_profile": "python_object",
                            "serialization_profile": "copy",
                        },
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def create_model_from_python_obj(self, data: Any) -> FileModel:

        if isinstance(data, Mapping):
            return FileModel(**data)
        if isinstance(data, str):
            return FileModel.load_file(source=data)
        else:
            raise Exception(f"Can't create FileModel from data of type '{type(data)}'.")

    def _pretty_print_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: Any = value.data
        max_lines = render_config.get("max_lines", 34)
        try:
            lines = []
            with open(data.path, "r", encoding="utf-8") as f:
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

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: FileModel = value.data
        max_lines = render_config.get("max_lines", 34)
        try:
            lines = []
            with open(data.path, "r", encoding="utf-8") as f:
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

    def serialize(self, data: FileBundle) -> "SerializedData":

        file_data: Dict[str, Any] = {}
        file_metadata = {}
        for rel_path, file in data.included_files.items():
            file_data[rel_path] = {"type": "file", "codec": "raw", "file": file.path}
            file_metadata[rel_path] = {
                "file_name": file.file_name,
                # "import_time": file.import_time,
            }

        metadata: Dict[str, Any] = {
            "included_files": file_metadata,
            "bundle_name": data.bundle_name,
            # "import_time": data.import_time,
            "size": data.size,
            "number_of_files": data.number_of_files,
        }

        assert "__file_metadata__" not in file_data
        file_data["__file_metadata__"] = {
            "type": "inline-json",
            "codec": "json",
            "inline_data": metadata,
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": file_data,
            "serialization_profile": "copy",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "deserialize.file_bundle",
                        "module_config": {
                            "value_type": "file_bundle",
                            "target_profile": "python_object",
                            "serialization_profile": "copy",
                        },
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def parse_python_obj(self, data: Any) -> FileBundle:

        if isinstance(data, FileBundle):
            return data
        elif isinstance(data, str):
            return FileBundle.import_folder(source=data)
        else:
            raise Exception(
                f"Can't create FileBundle from data of type '{type(data)}'."
            )

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        bundle: FileBundle = value.data
        renderable = bundle.create_renderable(**render_config)
        return renderable
