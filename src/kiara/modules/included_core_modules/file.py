# -*- coding: utf-8 -*-
from typing import Any, Mapping

from kiara import KiaraModule
from kiara.models.filesystem import FileModel
from kiara.models.module.persistence import (
    ByteProvisioningStrategy,
    BytesStructure,
    LoadConfig,
)
from kiara.models.values.value import Value, ValueSet
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

    def process(self, inputs: ValueSet, outputs: ValueSet):

        path = inputs.get_value_data("path")

        file = FileModel.load_file(source=path)
        outputs.set_value("file", file)


class SaveFileToStoreModule(PersistValueModule):

    _module_type_name = "file.save_to_data_store"

    def get_persistence_target_name(self) -> str:
        return "data_store"

    def get_persistence_format_name(self) -> str:
        return "file"

    def data_type__file(
        self, value: Value, persistence_config: Mapping[str, Any]
    ) -> LoadConfig:
        """Persist single files into a local kiara data store."""

        file: FileModel = value.data

        bytes_structure_data = {file.file_name: file.path}
        bytes_structure = BytesStructure.construct(
            data_type="file", bytes_map=bytes_structure_data
        )

        load_config_data = {
            "provisioning_strategy": ByteProvisioningStrategy.LINK_FOLDER,
            "module_type": "import.file",
            "inputs": {"base_folder": "${PROVISIONING_PATH}", "path": file.file_name},
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
