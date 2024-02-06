# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Mapping, Type, Union

import humanfriendly
import structlog
from pydantic import Field
from rich import box
from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.table import Table

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType, KiaraModelValueBaseType
from kiara.models.filesystem import KiaraFile, KiaraFileBundle
from kiara.models.values.value import Value
from kiara.utils.output import create_table_from_data_and_schema

if TYPE_CHECKING:
    from kiara.models.values.value import SerializedData

logger = structlog.getLogger()


class FileTypeConfig(DataTypeConfig):

    content_type: Union[str, None] = Field(
        description="The content type of this file.", default=None
    )


SUPPORTED_FILE_TYPES = ["csv", "json", "text", "binary"]


class FileValueType(KiaraModelValueBaseType[KiaraFile, FileTypeConfig]):
    """A file."""

    _data_type_name: ClassVar[str] = "file"

    @classmethod
    def retrieve_available_type_profiles(cls) -> Mapping[str, Mapping[str, Any]]:
        result = {}
        for ft in SUPPORTED_FILE_TYPES:
            result[f"{ft}_file"] = {"content_type": ft}
        return result

    @classmethod
    def python_class(cls) -> Type:
        return KiaraFile

    @classmethod
    def data_type_config_class(cls) -> Type[FileTypeConfig]:
        return FileTypeConfig

    def serialize(self, data: KiaraFile) -> "SerializedData":

        # metadata = orjson_dumps(data.metadata)
        # metadata_schemas = orjson_dumps(data.metadata_schema)
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
                    "metadata": data.metadata,
                    "metadata_schemas": data.metadata_schemas,
                },
            },
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.model_dump(),
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

    def create_model_from_python_obj(self, data: Any) -> KiaraFile:

        if isinstance(data, Mapping):
            return KiaraFile(**data)
        if isinstance(data, str):
            return KiaraFile.load_file(source=data)
        else:
            raise Exception(f"Can't create FileModel from data of type '{type(data)}'.")

    def _pretty_print_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: KiaraFile = value.data
        max_lines = render_config.get("max_lines", 34)
        try:
            lines = []
            with open(data.path, "r", encoding="utf-8") as f:
                for idx, line in enumerate(f):
                    if idx > max_lines:
                        lines.append("...\n")
                        lines.append("...")
                        break
                    lines.append(line)

            # TODO: syntax highlighting
            return "\n".join(lines)
        except UnicodeDecodeError:
            # found non-text data
            lines = [
                "Binary file or non-utf8 enconding, not printing content...",
                "",
                "[b]File metadata:[/b]",
                "",
                data.model_dump_json(indent=2),
            ]
            return "\n".join(lines)

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: KiaraFile = value.data
        max_lines = render_config.get("max_lines", 34)
        try:
            lines = []
            with open(data.path, "r", encoding="utf-8") as f:
                for idx, line in enumerate(f):
                    if idx > max_lines:
                        lines.append("...")
                        lines.append("...")
                        break
                    lines.append(line.rstrip())

            preview: RenderableType = Group(*lines)
        except UnicodeDecodeError:
            # found non-text data
            lines = [
                "",
                "[b]File metadata:[/b]",
                "",
                data.model_dump_json(indent=2),
            ]
            preview = Panel(
                "Binary file or non-utf8 enconding, not printing content...",
                box=box.HORIZONTALS,
                padding=(2, 2),
            )

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")

        table.add_row("Preview", preview)
        if data.metadata:
            metadata_table = create_table_from_data_and_schema(
                data=data.metadata, schema=data.metadata_schemas
            )
            table.add_row("Metadata", metadata_table)

        return table


class FileBundleValueType(AnyType[KiaraFileBundle, FileTypeConfig]):
    """A bundle of files (like a folder, zip archive, etc.)."""

    _data_type_name: ClassVar[str] = "file_bundle"

    @classmethod
    def retrieve_available_type_profiles(cls) -> Mapping[str, Mapping[str, Any]]:
        result = {}
        for ft in SUPPORTED_FILE_TYPES:
            result[f"{ft}_file_bundle"] = {"content_type": ft}
        return result

    @classmethod
    def python_class(cls) -> Type:
        return KiaraFileBundle

    @classmethod
    def data_type_config_class(cls) -> Type[FileTypeConfig]:
        return FileTypeConfig

    def serialize(self, data: KiaraFileBundle) -> "SerializedData":

        file_data: Dict[str, Any] = {}
        file_metadata = {}
        for rel_path, file in data.included_files.items():
            file_data[rel_path] = {"type": "file", "codec": "raw", "file": file.path}
            file_metadata[rel_path] = {
                "file_name": file.file_name,
                "size": file.size,
                "mime_type": file.mime_type,
                "metadata": file.metadata,
                "metadata_schemas": file.metadata_schemas,
            }

        # bundle_metadata = orjson_dumps(data.metadata)
        # bundle_metadata_schema = orjson_dumps(data.metadata_schema)
        metadata: Dict[str, Any] = {
            "included_files": file_metadata,
            "bundle_name": data.bundle_name,
            # "import_time": data.import_time,
            "size": data.size,
            "number_of_files": data.number_of_files,
            "metadata": data.metadata,
            "metadata_schemas": data.metadata_schemas,
        }

        assert "__file_metadata__" not in file_data

        file_data["__file_metadata__"] = {
            "type": "inline-json",
            "codec": "json",
            "inline_data": metadata,
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.model_dump(),
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

    def parse_python_obj(self, data: Any) -> KiaraFileBundle:

        if isinstance(data, KiaraFileBundle):
            return data
        elif isinstance(data, str):
            return KiaraFileBundle.import_folder(source=data)
        else:
            raise Exception(
                f"Can't create FileBundle from data of type '{type(data)}'."
            )

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        bundle: KiaraFileBundle = value.data
        renderable = bundle.create_renderable(**render_config)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")

        table.add_row("File bundle info", renderable)
        if bundle.metadata:
            metadata_table = create_table_from_data_and_schema(
                data=bundle.metadata, schema=bundle.metadata_schemas
            )
            table.add_row("Metadata", metadata_table)

        return table

    def _pretty_print_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        bundle: KiaraFileBundle = value.data
        result = []
        result.append(f"File bundle '{bundle.bundle_name}")
        result.append(f"  size: {humanfriendly.format_size(bundle.size)}")
        result.append("  contents:")
        for rel_path, file in bundle.included_files.items():
            result.append(f"    - {rel_path}: {file.file_name}")

        return "\n".join(result)
