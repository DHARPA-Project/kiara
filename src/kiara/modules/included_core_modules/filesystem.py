# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import orjson
import os
import shutil
from pydantic import Field
from typing import Any, List, Mapping, Type, Union

from kiara import KiaraModuleConfig
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import FileBundle, FileModel, FolderImportConfig
from kiara.models.values.value import SerializedData, ValueMap
from kiara.modules import (
    DEFAULT_NO_IDEMPOTENT_MODULE_CHARACTERISTICS,
    KiaraModule,
    ModuleCharacteristics,
    ValueMapSchema,
)
from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara.modules.included_core_modules.serialization import DeserializeValueModule


class ImportFileModule(KiaraModule):
    """Import a file from the local filesystem."""

    _module_type_name = "import.file"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        return {"path": {"type": "string", "doc": "The local path to the file."}}

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        return {"file": {"type": "file", "doc": "The loaded files."}}

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return ModuleCharacteristics(is_idempotent=False)

    def process(self, inputs: ValueMap, outputs: ValueMap):

        path = inputs.get_value_data("path")

        file = FileModel.load_file(source=path)
        outputs.set_value("file", file)


class DeserializeFileModule(DeserializeValueModule):
    """Deserialize data to a 'file' value instance."""

    _module_type_name = "deserialize.file"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": FileModel}

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

        fm = FileModel.load_file(
            source=file,
            file_name=file_metadata["file_name"],
        )
        return fm


class ImportFileBundleConfig(KiaraModuleConfig):

    include_file_types: Union[None, List[str]] = Field(
        description="File types to include.", default=None
    )
    exclude_file_types: Union[None, List[str]] = Field(
        description="File types to include.", default=None
    )


class ImportFileBundleModule(KiaraModule):
    """Import a folder (file_bundle) from the local filesystem."""

    _module_type_name = "import.file_bundle"
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

        file_bundle = FileBundle.import_folder(source=path, import_config=config)
        outputs.set_value("file_bundle", file_bundle)


class DeserializeFileBundleModule(DeserializeValueModule):
    """Deserialize data to a 'file' value instance."""

    _module_type_name = "deserialize.file_bundle"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": FileBundle}

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

            chunks = data.get_serialized_data(rel_path)
            assert chunks.get_number_of_chunks() == 1

            files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
            assert len(files) == 1

            file: str = files[0]  # type: ignore
            file_name = file_metadata[rel_path]["file_name"]
            # import_time = file_metadata[rel_path]["import_time"]
            fm = FileModel.load_file(source=file, file_name=file_name)
            included_files[rel_path] = fm

        fb = FileBundle(
            included_files=included_files,
            bundle_name=bundle_name,
            # import_time=bundle_import_time,
            number_of_files=number_of_files,
            size=sum_size,
        )
        return fb


class ExportFileModule(DataExportModule):
    """Export files."""

    _module_type_name = "export.file"

    def export__file__as__file(self, value: FileModel, base_path: str, name: str):

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

        file_bundle: FileBundle = inputs.get_value_data("file_bundle")
        path: str = inputs.get_value_data("path")

        if path not in file_bundle.included_files.keys():
            raise KiaraProcessingException(
                f"Can't pick file '{path}' from file bundle: file not available."
            )

        file: FileModel = file_bundle.included_files[path]

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

        file_bundle: FileBundle = inputs.get_value_data("file_bundle")
        sub_path: str = inputs.get_value_data("sub_path")

        result = {}
        for path, file in file_bundle.included_files.items():
            if path.startswith(sub_path):
                result[path] = file

        if not result:
            raise KiaraProcessingException(
                f"Can't pick sub-folder '{sub_path}' from file bundle: no matches."
            )

        new_file_bundle: FileBundle = FileBundle.create_from_file_models(
            result, bundle_name=f"{file_bundle.bundle_name}_{sub_path}"
        )

        outputs.set_value("file_bundle", new_file_bundle)
