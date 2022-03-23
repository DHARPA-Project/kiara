# -*- coding: utf-8 -*-
import orjson.orjson
import structlog
from typing import Any, Mapping, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType, KiaraModelValueType
from kiara.models.filesystem import FileBundle, FileModel
from kiara.models.values.value import Value
from kiara.utils import orjson_dumps

logger = structlog.getLogger()


class FileValueType(KiaraModelValueType[FileModel, DataTypeConfig]):
    """A file."""

    _data_type_name = "file"

    @classmethod
    def python_class(cls) -> Type:
        return FileModel

    @classmethod
    def data_type_config_class(cls) -> Type[DataTypeConfig]:
        return DataTypeConfig

    @classmethod
    def value_class(cls) -> Type[Value]:
        return Value

    def calculate_size(self, data: FileModel) -> int:
        return data.size

    def calculate_hash(self, data: FileModel) -> int:
        return data.file_hash

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


class FileBundleValueType(AnyType[FileModel, DataTypeConfig]):
    """A bundle of files (like a folder, zip archive, etc.)."""

    _data_type_name = "file_bundle"

    @classmethod
    def python_class(cls) -> Type:
        return FileBundle

    @classmethod
    def value_class(cls) -> Type[Value]:
        return Value

    def calculate_size(self, data: FileBundle) -> int:
        return data.size

    def calculate_hash(self, data: FileBundle) -> int:
        return data.file_bundle_hash

    def create_model_from_python_obj(self, data: Any) -> FileModel:

        if isinstance(data, str):
            return FileBundle.import_folder(source=data)
        else:
            raise Exception(
                f"Can't create FileBundle from data of type '{type(data)}'."
            )

    def render_as__renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        max_no_included_files = render_config.get("max_no_files", 40)

        data: FileBundle = value.data
        pretty = data.dict(exclude={"included_files"})
        files = list(data.included_files.keys())
        if max_no_included_files >= 0:
            if len(files) > max_no_included_files:
                half = int((max_no_included_files - 1) / 2)
                head = files[0:half]
                tail = files[-1 * half :]  # noqa
                files = (
                    head
                    + ["..... output skipped .....", "..... output skipped ....."]
                    + tail
                )
        pretty["included_files"] = files
        return orjson_dumps(pretty, option=orjson.orjson.OPT_INDENT_2)
