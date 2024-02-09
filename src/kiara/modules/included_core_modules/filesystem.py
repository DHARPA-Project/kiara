# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
import shutil
from typing import Any, List, Mapping, Type, Union

import orjson
from pydantic import Field

from kiara.api import KiaraModuleConfig
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import FolderImportConfig, KiaraFile, KiaraFileBundle
from kiara.models.values.value import SerializedData, ValueMap
from kiara.modules import (
    DEFAULT_NO_IDEMPOTENT_MODULE_CHARACTERISTICS,
    KiaraModule,
    ModuleCharacteristics,
    ValueMapSchema,
)
from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara.modules.included_core_modules.serialization import DeserializeValueModule


class ImportLocalFileModule(KiaraModule):
    """Import a file from the local filesystem."""

    _module_type_name = "import.local.file"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "path": {
                "type": "string",
                "doc": "The local path to the file (absolute, or relative to current directory.",
            }
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {"file": {"type": "file", "doc": "The loaded files."}}

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return ModuleCharacteristics(is_idempotent=False)

    def process(self, inputs: ValueMap, outputs: ValueMap):

        path = inputs.get_value_data("path")

        file = KiaraFile.load_file(source=path)
        outputs.set_value("file", file)


class DeserializeFileModule(DeserializeValueModule):
    """Deserialize data to a 'file' value instance."""

    _module_type_name = "deserialize.file"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": KiaraFile}

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "file"

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "copy"

    def to__python_object(self, data: SerializedData, **config: Any):

        keys = list(data.get_keys())
        keys.remove("__file_metadata__")
        assert len(keys) == 1

        file_metadata_chunks = data.get_serialized_data("__file_metadata__")
        assert file_metadata_chunks.get_number_of_chunks() == 1
        file_metadata_json = list(file_metadata_chunks.get_chunks(as_files=False))
        assert len(file_metadata_json) == 1
        file_metadata = orjson.loads(file_metadata_json[0])

        chunks = data.get_serialized_data(keys[0])
        assert chunks.get_number_of_chunks() == 1

        files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
        assert len(files) == 1

        file: str = files[0]  # type: ignore

        _file_name = file_metadata.pop("file_name")
        _file_metadata = file_metadata.pop("metadata")
        _file_metadata_schemas = file_metadata.pop("metadata_schemas")

        fm = KiaraFile.load_file(
            source=file,
            file_name=_file_name,
        )
        fm.metadata = _file_metadata
        fm.metadata_schemas = _file_metadata_schemas
        return fm


class ImportFileBundleConfig(KiaraModuleConfig):

    include_file_types: Union[None, List[str]] = Field(
        description="File types to include. Type is list of strings, which will be matched using 'endswith' test.",
        default=None,
    )
    exclude_file_types: Union[None, List[str]] = Field(
        description="File types to exclude. Type is list of strings, which will be matched with the 'endswith' test.",
        default=None,
    )


class ImportLocalFileBundleModule(KiaraModule):
    """Import a folder (file_bundle) from the local filesystem."""

    _module_type_name = "import.local.file_bundle"
    _config_cls = ImportFileBundleConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "path": {"type": "string", "doc": "The local path of the folder to import."}
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "file_bundle": {"type": "file_bundle", "doc": "The imported file bundle."}
        }

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return DEFAULT_NO_IDEMPOTENT_MODULE_CHARACTERISTICS

    def process(self, inputs: ValueMap, outputs: ValueMap):

        path = inputs.get_value_data("path")

        include = self.get_config_value("include_file_types")
        exclude = self.get_config_value("exclude_file_types")

        config = FolderImportConfig(include_files=include, exclude_files=exclude)

        file_bundle = KiaraFileBundle.import_folder(source=path, import_config=config)
        outputs.set_value("file_bundle", file_bundle)


class DeserializeFileBundleModule(DeserializeValueModule):
    """Deserialize data to a 'file' value instance."""

    _module_type_name = "deserialize.file_bundle"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": KiaraFileBundle}

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "file_bundle"

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "copy"

    def to__python_object(self, data: SerializedData, **config: Any):

        keys = list(data.get_keys())
        keys.remove("__file_metadata__")

        file_metadata_chunks = data.get_serialized_data("__file_metadata__")
        assert file_metadata_chunks.get_number_of_chunks() == 1
        file_metadata_json = list(file_metadata_chunks.get_chunks(as_files=False))
        assert len(file_metadata_json) == 1
        metadata = orjson.loads(file_metadata_json[0])
        file_metadata = metadata["included_files"]
        bundle_name = metadata["bundle_name"]
        # bundle_import_time = metadata["import_time"]
        sum_size = metadata["size"]
        number_of_files = metadata["number_of_files"]

        included_files = {}
        for rel_path in keys:

            if "size" not in file_metadata[rel_path].keys():
                # old style, can be removed at some point
                # file metadata was added feb 2024

                chunks = data.get_serialized_data(rel_path)
                assert chunks.get_number_of_chunks() == 1
                files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
                assert len(files) == 1

                file: str = files[0]  # type: ignore
                file_name = file_metadata[rel_path]["file_name"]
                # import_time = file_metadata[rel_path]["import_time"]
                fm = KiaraFile.load_file(source=file, file_name=file_name)
            else:
                fm = KiaraFile(**file_metadata[rel_path])

                def _load_file():
                    chunks = data.get_serialized_data(rel_path)
                    assert chunks.get_number_of_chunks() == 1
                    files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
                    assert len(files) == 1
                    return files[0]

                fm._path_resolver = _load_file

            included_files[rel_path] = fm

        fb_metadata = metadata.pop("metadata")
        fb_metadata_schemas = metadata.pop("metadata_schemas")

        fb = KiaraFileBundle(
            included_files=included_files,
            bundle_name=bundle_name,
            # import_time=bundle_import_time,
            number_of_files=number_of_files,
            size=sum_size,
            metadata=fb_metadata,
            metadata_schemas=fb_metadata_schemas,
        )
        return fb


class ExportFileModule(DataExportModule):
    """Export files."""

    _module_type_name = "export.file"

    def export__file__as__file(self, value: KiaraFile, base_path: str, name: str):

        target_path = os.path.join(base_path, value.file_name)

        shutil.copy2(value.path, target_path)

        return {"files": target_path}


class PickFileFromFileBundleModule(KiaraModule):
    """Pick a single file from a file_bundle value."""

    _module_type_name = "file_bundle.pick.file"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "file_bundle": {"type": "file_bundle", "doc": "The file bundle."},
            "path": {"type": "string", "doc": "The relative path of the file to pick."},
        }

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {"file": {"type": "file", "doc": "The file."}}

    def process(self, inputs: ValueMap, outputs: ValueMap):

        file_bundle: KiaraFileBundle = inputs.get_value_data("file_bundle")
        path: str = inputs.get_value_data("path")

        if path not in file_bundle.included_files.keys():
            raise KiaraProcessingException(
                f"Can't pick file '{path}' from file bundle: file not available."
            )

        file: KiaraFile = file_bundle.included_files[path]

        outputs.set_value("file", file)


class PickSubBundle(KiaraModule):
    """Pick a sub-folder from a file_bundle, resulting in a new file_bundle."""

    _module_type_name = "file_bundle.pick.sub_folder"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {
            "file_bundle": {"type": "file_bundle", "doc": "The file bundle."},
            "sub_path": {
                "type": "string",
                "doc": "The relative path of the sub-folder to pick.",
            },
        }

    def create_outputs_schema(self) -> ValueMapSchema:
        return {
            "file_bundle": {
                "type": "file_bundle",
                "doc": "The picked (sub-)file_bundle.",
            }
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        file_bundle: KiaraFileBundle = inputs.get_value_data("file_bundle")
        sub_path: str = inputs.get_value_data("sub_path")

        result = {}
        for path, file in file_bundle.included_files.items():
            if path.startswith(sub_path):
                result[path] = file

        if not result:
            raise KiaraProcessingException(
                f"Can't pick sub-folder '{sub_path}' from file bundle: no matches."
            )

        new_file_bundle: KiaraFileBundle = KiaraFileBundle.create_from_file_models(
            result, bundle_name=f"{file_bundle.bundle_name}_{sub_path}"
        )

        outputs.set_value("file_bundle", new_file_bundle)
