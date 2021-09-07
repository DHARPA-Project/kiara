# -*- coding: utf-8 -*-
import typing

from kiara.data import Value
from kiara.metadata import MetadataModel
from kiara.operations import Operation
from kiara.utils.class_loading import find_all_metadata_schemas

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.operations.extract_metadata import (
        ExtractMetadataModule,
        ExtractMetadataOperationType,
    )


class MetadataMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: "Kiara" = kiara
        self._metadata_models: typing.Optional[
            typing.Dict[str, typing.Type[MetadataModel]]
        ] = None

    @property
    def all_schemas(self) -> typing.Mapping[str, typing.Type[MetadataModel]]:

        if self._metadata_models is None:
            self._metadata_models = find_all_metadata_schemas()
        return self._metadata_models

    def find_all_schemas_for_package(
        self, package_name: str
    ) -> typing.Dict[str, typing.Type[MetadataModel]]:

        result = {}
        for name, schema in self.all_schemas.items():
            schema_md = schema.get_model_cls_metadata()
            package = schema_md.context.labels.get("package")
            if package == package_name:
                result[name] = schema

        return result

    def get_metadata_keys_for_type(self, value_type: str) -> typing.Set[str]:

        metadata_operations: ExtractMetadataOperationType = self._kiara.operation_mgmt.get_operations("extract_metadata")  # type: ignore

        all_profiles_for_type: typing.Mapping[
            str, Operation
        ] = metadata_operations.get_all_operations_for_type(value_type)

        if not all_profiles_for_type:
            return set()
        else:
            return set(all_profiles_for_type.keys())

    def get_value_metadata(
        self, value: Value, *metadata_keys: str, also_return_schema: bool = False
    ):

        if value.is_none:
            return {
                k: {"metadata_item": {}, "metadata_schema": ""} for k in metadata_keys
            }

        value_type = value.value_schema.type
        # TODO: validate type exists

        all_metadata_keys_for_type = self.get_metadata_keys_for_type(
            value_type=value_type
        )
        # all_profiles_for_type = self._kiara.operations.type_operations.extract_metadata_profiles.get(
        #     value_type, None
        # )
        # if all_profiles_for_type is None:
        #     all_profiles_for_type = {}

        if not metadata_keys:
            _metadata_keys = set(all_metadata_keys_for_type)
            # add existing, externally added metadata
            for key in value.metadata.keys():
                _metadata_keys.add(key)
        elif isinstance(metadata_keys, str):
            _metadata_keys = {metadata_keys}
        elif isinstance(metadata_keys, typing.Iterable):
            _metadata_keys = set(metadata_keys)
        else:
            raise TypeError(f"Invalid type for metadata keys: {type(metadata_keys)}")

        result = {}
        missing = []

        for md_key in all_metadata_keys_for_type:
            if md_key not in value.metadata.keys():
                missing.append(md_key)
            else:
                result[md_key] = value.metadata[md_key]

        extract_metadata_ops: ExtractMetadataOperationType = self._kiara.operation_mgmt.get_operations(  # type: ignore
            "extract_metadata"
        )
        value_md_ops = extract_metadata_ops.get_all_operations_for_type(
            value_type=value_type
        )

        for mk in missing:
            if mk not in all_metadata_keys_for_type:
                raise Exception(
                    f"Can't extract metadata profile '{mk}' for type '{value_type}': metadata profile does not exist (for this type, anyway)."
                )

            op_config = value_md_ops.get(mk, None)
            if op_config is None:
                raise Exception(
                    f"Can't extract metadata profile '{mk}' for type '{value_type}': metadata profile does not exist (for this type, anyway). This is a bug."
                )

            md_module: ExtractMetadataModule = op_config.module  # type: ignore
            md_result = md_module.run(_attach_lineage=False, value_item=value)
            result[mk] = md_result.get_all_value_data()

        if also_return_schema:
            return result
        else:
            return {k: v["metadata_item"] for k, v in result.items()}
