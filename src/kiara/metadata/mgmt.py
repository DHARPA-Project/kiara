# -*- coding: utf-8 -*-
import typing

from kiara.data import Value
from kiara.data.operations import OperationType
from kiara.metadata import MetadataModel
from kiara.utils.class_loading import find_all_metadata_schemas

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


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

        all_profiles_for_type: typing.Mapping[
            str, OperationType
        ] = self._kiara.data_operations.operations.get(value_type, {}).get(
            "extract_metadata", {}
        )

        if not all_profiles_for_type:
            return set()
        else:
            return set(all_profiles_for_type.keys())

    def get_value_metadata(
        self, value: Value, *metadata_keys: str, also_return_schema: bool = False
    ):

        value_type = value.value_schema.type
        # TODO: validate type exists

        all_metadata_keys_for_type = self.get_metadata_keys_for_type(
            value_type=value_type
        )
        # all_profiles_for_type = self._kiara.data_operations.extract_metadata_profiles.get(
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

        for mk in missing:
            if mk not in all_metadata_keys_for_type:
                raise Exception(
                    f"Can't extract metadata profile '{mk}' for type '{value_type}': metadata profile does not exist (for this type, anyway)."
                )

            md_result = self._kiara._operation_mgmt.run(
                operation_name="extract_metadata", operation_id=mk, value=value
            )
            result[mk] = md_result.get_all_value_data()

        if also_return_schema:
            return result
        else:
            return {k: v["item_metadata"] for k, v in result.items()}
