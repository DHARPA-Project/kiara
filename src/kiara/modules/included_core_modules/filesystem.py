# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import orjson
from typing import Any, Mapping, Type

from kiara import KiaraModule
from kiara.models.filesystem import FileBundle, FileModel
from kiara.models.values.value import SerializedData, ValueMap
from kiara.modules import ValueSetSchema
from kiara.modules.included_core_modules.serialization import DeserializeValueModule


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
            import_time=file_metadata["import_time"],
        )
        return fm


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
        bundle_import_time = metadata["import_time"]
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
            import_time = file_metadata[rel_path]["import_time"]
            fm = FileModel.load_file(
                source=file, file_name=file_name, import_time=import_time
            )
            included_files[rel_path] = fm

        fb = FileBundle(
            included_files=included_files,
            bundle_name=bundle_name,
            import_time=bundle_import_time,
            number_of_files=number_of_files,
            size=sum_size,
        )
        return fb


# class LoadFileBundleFromStoreModule(KiaraModule):
#
#     _module_type_name = "load.file_bundle.from_data_store"
#
#     def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
#         return ModuleCharacteristics(is_internal=True)
#
#     def create_inputs_schema(
#         self,
#     ) -> ValueSetSchema:
#
#         return {
#             "inline_data": {"type": "any", "doc": "The file bundle metadata."},
#             "path": {
#                 "type": "string",
#                 "doc": "The path to the provisioned file bundle.",
#             },
#         }
#
#     def create_outputs_schema(
#         self,
#     ) -> ValueSetSchema:
#
#         return {
#             "file_bundle": {
#                 "type": "file_bundle",
#                 "doc": "The loaded file_bundle value.",
#             }
#         }
#
#     def process(self, inputs: ValueMap, outputs: ValueMap):
#
#         path = inputs.get_value_data("path")
#
#         # bundle_name = inputs.get_value_data("bundle_name")
#         # import_time_str = inputs.get_value_data("import_time")
#         # import_time = parser.parse(import_time_str)
#         bundle_data = inputs.get_value_data("inline_data")
#
#         file_bundle = FileBundle(**bundle_data)
#
#         file_bundle._path = path
#         for rel_path, model in file_bundle.included_files.items():
#             model._path = os.path.join(path, rel_path)
#
#         outputs.set_value("file_bundle", file_bundle)


# class SaveFileBundleToStoreModule(PersistValueModule):
#
#     _module_type_name = "file_bundle.save_to_data_store"
#
#     def get_persistence_target_name(self) -> str:
#         return "data_store"
#
#     def get_persistence_format_name(self) -> str:
#         return "file_bundle"
#
#     def data_type__file_bundle(
#         self, value: Value, persistence_config: Mapping[str, Any]
#     ) -> Tuple[LoadConfig, BytesStructure]:
#         """Persist single files into a local kiara data store."""
#
#         file_bundle: FileBundle = value.data
#
#         bytes_structure_data: Dict[str, List[Union[str, bytes]]] = {}
#
#         for rel_path, file_model in file_bundle.included_files.items():
#             bytes_structure_data[rel_path] = [file_model.path]
#
#         bytes_structure = BytesStructure.construct(
#             data_type="file_bundle", data_type_config={}, chunk_map=bytes_structure_data
#         )
#
#         load_config_data = {
#             "provisioning_strategy": ByteProvisioningStrategy.LINK_FOLDER,
#             "module_type": "load.file_bundle.from_data_store",
#             "inputs": {
#                 "inline_data": LOAD_CONFIG_PLACEHOLDER,
#                 "path": LOAD_CONFIG_PLACEHOLDER,
#                 "bytes_structure": LOAD_CONFIG_PLACEHOLDER,
#             },
#             "inline_data": file_bundle.dict(),
#             "output_name": "file_bundle",
#         }
#
#         load_config = LoadConfig(**load_config_data)
#         return load_config, bytes_structure
