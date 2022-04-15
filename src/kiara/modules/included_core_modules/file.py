# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from dateutil import parser
from typing import Any, List, Mapping, Tuple, Union

from kiara import KiaraModule
from kiara.models.filesystem import FileModel
from kiara.models.module.persistence import (
    ByteProvisioningStrategy,
    BytesStructure,
    LoadConfig,
)
from kiara.models.values.value import Value, ValueMap
from kiara.modules import ValueSetSchema
from kiara.modules.included_core_modules.persistence import PersistValueModule


class ImportFileModule(KiaraModule):

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
                "bytes_structure": "__dummy__",
            },
            "output_name": "file",
        }

        load_config = LoadConfig(**load_config_data)
        return load_config, bytes_structure

    # def data_type__table(self, value: Value, config: Any) -> LoadConfig:
    #
    #     import pyarrow as pa
    #     table: pa.Table  = value.data
    #
    #     pa.feather.write_feather(table)
