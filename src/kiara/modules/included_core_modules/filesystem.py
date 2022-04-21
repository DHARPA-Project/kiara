# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
from dateutil import parser
from typing import Any, Dict, List, Mapping, Tuple, Union

from kiara import KiaraModule
from kiara.defaults import LOAD_CONFIG_PLACEHOLDER
from kiara.models.filesystem import FileBundle, FileModel
from kiara.models.module.persistence import (
    ByteProvisioningStrategy,
    BytesStructure,
    LoadConfig,
)
from kiara.models.values.value import Value, ValueMap
from kiara.modules import ModuleCharacteristics, ValueSetSchema
from kiara.modules.included_core_modules.persistence import PersistValueModule


class ImportFileModule(KiaraModule):
    """Import a file from the local filesystem."""

    _module_type_name = "import.file"

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        return {"path": {"type": "string", "doc": "The local path to the file."}}

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        return {"file": {"type": "file", "doc": "The loaded files."}}

    def process(self, inputs: ValueMap, outputs: ValueMap):

        path = inputs.get_value_data("path")

        file = FileModel.load_file(source=path)
        outputs.set_value("file", file)


class LoadFileFromStoreModule(KiaraModule):

    _module_type_name = "load.file.from_data_store"

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return ModuleCharacteristics(is_internal=True)

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        return {
            "file_name": {"type": "string", "doc": "The name of the file."},
            "import_time": {
                "type": "string",
                "doc": "The (original) import time of the file.",
            },
            "bytes_structure": {
                "type": "any",
                "doc": "The bytes that make up the file.",
            },
        }

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        return {"file": {"type": "file", "doc": "The loaded files."}}

    def process(self, inputs: ValueMap, outputs: ValueMap):

        file_name = inputs.get_value_data("file_name")
        import_time_str = inputs.get_value_data("import_time")

        bytes_structure: BytesStructure = inputs.get_value_data("bytes_structure")
        assert len(bytes_structure.chunk_map) == 1

        import_time = parser.parse(import_time_str)

        file_chunks = bytes_structure.chunk_map[file_name]
        assert len(file_chunks) == 1
        chunk = file_chunks[0]
        assert isinstance(chunk, str)
        file = FileModel.load_file(
            source=chunk,
            file_name=file_name,
            import_time=import_time,
        )
        outputs.set_value("file", file)


class SaveFileToStoreModule(PersistValueModule):

    _module_type_name = "file.save_to_data_store"

    def get_persistence_target_name(self) -> str:
        return "data_store"

    def get_persistence_format_name(self) -> str:
        return "file"

    def data_type__file(
        self, value: Value, persistence_config: Mapping[str, Any]
    ) -> Tuple[LoadConfig, BytesStructure]:
        """Persist single files into a local kiara data store."""

        file: FileModel = value.data

        bytes_structure_data: Mapping[str, List[Union[str, bytes]]] = {
            file.file_name: [file.path]
        }
        bytes_structure = BytesStructure.construct(
            data_type="file", data_type_config={}, chunk_map=bytes_structure_data
        )

        load_config_data = {
            "provisioning_strategy": ByteProvisioningStrategy.FILE_PATH_MAP,
            "module_type": "load.file.from_data_store",
            "inputs": {
                "file_name": file.file_name,
                "import_time": str(file.import_time),
                "bytes_structure": LOAD_CONFIG_PLACEHOLDER,
            },
            "output_name": "file",
        }

        load_config = LoadConfig(**load_config_data)
        return load_config, bytes_structure


class ImportFileBundleModule(KiaraModule):
    """Import a folder (file_bundle) from the local filesystem."""

    _module_type_name = "import.file_bundle"

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        return {
            "path": {"type": "string", "doc": "The local path of the folder to import."}
        }

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        return {
            "file_bundle": {"type": "file_bundle", "doc": "The imported file bundle."}
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        path = inputs.get_value_data("path")

        file_bundle = FileBundle.import_folder(source=path)
        outputs.set_value("file_bundle", file_bundle)


class LoadFileBundleFromStoreModule(KiaraModule):

    _module_type_name = "load.file_bundle.from_data_store"

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return ModuleCharacteristics(is_internal=True)

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        return {
            "inline_data": {"type": "any", "doc": "The file bundle metadata."},
            "path": {
                "type": "string",
                "doc": "The path to the provisioned file bundle.",
            },
        }

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        return {
            "file_bundle": {
                "type": "file_bundle",
                "doc": "The loaded file_bundle value.",
            }
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        path = inputs.get_value_data("path")

        # bundle_name = inputs.get_value_data("bundle_name")
        # import_time_str = inputs.get_value_data("import_time")
        # import_time = parser.parse(import_time_str)
        bundle_data = inputs.get_value_data("inline_data")

        file_bundle = FileBundle(**bundle_data)

        file_bundle._path = path
        for rel_path, model in file_bundle.included_files.items():
            model._path = os.path.join(path, rel_path)

        outputs.set_value("file_bundle", file_bundle)


class SaveFileBundleToStoreModule(PersistValueModule):

    _module_type_name = "file_bundle.save_to_data_store"

    def get_persistence_target_name(self) -> str:
        return "data_store"

    def get_persistence_format_name(self) -> str:
        return "file_bundle"

    def data_type__file_bundle(
        self, value: Value, persistence_config: Mapping[str, Any]
    ) -> Tuple[LoadConfig, BytesStructure]:
        """Persist single files into a local kiara data store."""

        file_bundle: FileBundle = value.data

        bytes_structure_data: Dict[str, List[Union[str, bytes]]] = {}

        for rel_path, file_model in file_bundle.included_files.items():
            bytes_structure_data[rel_path] = [file_model.path]

        bytes_structure = BytesStructure.construct(
            data_type="file_bundle", data_type_config={}, chunk_map=bytes_structure_data
        )

        load_config_data = {
            "provisioning_strategy": ByteProvisioningStrategy.LINK_FOLDER,
            "module_type": "load.file_bundle.from_data_store",
            "inputs": {
                "inline_data": LOAD_CONFIG_PLACEHOLDER,
                "path": LOAD_CONFIG_PLACEHOLDER,
                "bytes_structure": LOAD_CONFIG_PLACEHOLDER,
            },
            "inline_data": file_bundle.dict(),
            "output_name": "file_bundle",
        }

        load_config = LoadConfig(**load_config_data)
        return load_config, bytes_structure
