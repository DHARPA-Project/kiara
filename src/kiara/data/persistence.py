# -*- coding: utf-8 -*-
import json
import os
import typing
import uuid
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field
from tzlocal import get_localzone

from kiara.data.operations import ModuleProfileConfig
from kiara.defaults import (
    KIARA_ALIAS_VALUE_FOLDER,
    KIARA_DATA_STORE,
    KIARA_METADATA_STORE,
)

if typing.TYPE_CHECKING:
    from kiara_modules.core.metadata_schemas import FileBundleMetadata, FileMetadata

    from kiara.data.values import Value
    from kiara.kiara import Kiara


class SnapshotMetadata(BaseModel):

    value_type: str = Field(description="The value type.")
    value_id: str = Field(description="The value id after the snapshot.")
    value_id_orig: str = Field(description="The value id before the snapshot.")
    snapshot_time: str = Field(description="The time the data was saved.")


class LoadConfig(ModuleProfileConfig):

    inputs: typing.Dict[str, typing.Any] = Field(
        description="The inputs to use when running this module.", default_factory=dict
    )
    output_name: str = Field(description="The name of the output field for the value.")


class DataStore(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._data_store: Path = Path(KIARA_DATA_STORE)
        self._metadata_store: Path = Path(KIARA_METADATA_STORE)
        self._alias_folder: Path = Path(KIARA_ALIAS_VALUE_FOLDER)

        os.makedirs(self._metadata_store, exist_ok=True)
        os.makedirs(self._alias_folder, exist_ok=True)
        self._aliases: typing.Optional[typing.Dict[str, str]] = None
        self._value_id_cache: typing.Optional[
            typing.Dict[str, typing.Optional[typing.Dict[str, typing.Any]]]
        ] = None

    @property
    def data_store_path(self) -> Path:
        return self._data_store

    def alias_available(self, alias: str):
        if alias in self.aliases.keys() or alias in self.value_ids:
            return False
        else:
            return True

    def check_existing_aliases(self, *aliases: str) -> typing.List[str]:

        invalid = []
        for alias in aliases:
            if not self.alias_available(alias):
                invalid.append(alias)
        return invalid

    def save_value(self, value: "Value") -> str:

        # save_config = self._kiara.data_operations.get_operation_config(value_type=value.type_name, operation_name="save_value", operation_id="data_store")
        # assert isinstance(save_config, SaveOperationType)

        op = self._kiara.data_operations.get_operation(
            value_type=value.type_name,
            operation_name="calculate_hash",
            operation_id="default",
        )
        if not op:
            new_value_id: str = str(uuid.uuid4())
        else:
            result = self._kiara.data_operations.run(
                operation_name="calculate_hash", operation_id="default", value=value
            )
            new_value_id = result.get_value_data("hash")

        # if value.type_name == "file":
        #     fm: FileMetadata = value.get_value_data()
        #     new_value_id = fm.file_hash
        #     if new_value_id in self.value_ids:
        #         # file with this hash was already stored, we don't need to do it again
        #         return new_value_id
        # elif value.type_name == "file_bundle":
        #     fbm: FileBundleMetadata = value.get_value_data()
        #     new_value_id = fbm.file_bundle_hash
        #     if new_value_id in self.value_ids:
        #         return new_value_id
        # else:
        #     new_value_id = str(uuid.uuid4())

        invalid = self.check_existing_aliases(*value.aliases)

        if invalid:
            raise Exception(
                f"Can't save value, alias(es) already registered: {', '.join(invalid)}"
            )

        target_path = os.path.join(self._data_store, new_value_id)
        metadata_path = os.path.join(
            self._metadata_store, f"{new_value_id}.{value.type_name}.metadata.json"
        )
        if os.path.exists(metadata_path):
            raise Exception(
                f"Can't save value, metadata file alrady exists: {metadata_path}"
            )

        op_config = self._kiara.data_operations.get_operation(
            value_type=value.type_name,
            operation_name="save_value",
            operation_id="data_store",
        )
        other_inputs = {op_config.target_name: target_path}  # type: ignore
        result = self._kiara.data_operations.run(
            operation_name="save_value",
            operation_id="data_store",
            value=value,
            other_inputs=other_inputs,
        )

        # file and file_bundle values are special cases, and we need to update their metadata before saving,
        # otherwise it'd point to the wrong path
        if value.type_name == "file":
            onboarded_file = result.get_value_data("file")
            orig_file: FileMetadata = value.get_value_data()
            orig_file.is_onboarded = True
            orig_file.path = onboarded_file.path
        elif value.type_name == "file_bundle":
            onboarded_bundle = result.get_value_data("file_bundle")
            orig_bundle: FileBundleMetadata = value.get_value_data()
            orig_bundle.included_files = onboarded_bundle.included_files
            for path, f in orig_bundle.included_files.items():
                f.is_onboarded = True

        load_config_value: Value = result.get_value_obj("load_config")
        metadata: typing.Dict[str, typing.Mapping[str, typing.Any]] = dict(
            value.get_metadata(also_return_schema=True)
        )
        assert "snapshot" not in metadata.keys()
        assert "load_config" not in metadata.keys()
        assert "value" not in metadata.keys()
        tz = get_localzone()
        local_dt = tz.localize(datetime.now(), is_dst=None)

        ssmd = SnapshotMetadata(
            value_type=value.type_name,
            value_id=new_value_id,
            value_id_orig=value.id,
            snapshot_time=str(local_dt),
        )
        metadata["snapshot"] = {
            "item_metadata": ssmd.dict(),
            "item_metadata_schema": ssmd.schema_json(),
        }
        load_config = load_config_value.get_value_data()
        metadata["load_config"] = {
            "item_metadata": load_config,
            "item_metadata_schema": LoadConfig.schema_json(),
        }
        metadata["value"] = {
            "item_metadata": value.value_metadata.dict(),
            "item_metadata_schema": value.value_metadata.schema_json(),
        }

        with open(metadata_path, "w") as _f:
            _f.write(json.dumps(metadata))

        for alias in value.aliases:
            alias_file = os.path.join(self._alias_folder, f"{alias}.metadata.json")
            os.symlink(metadata_path, alias_file)

        return ssmd.value_id

    @property
    def values_metadata(
        self,
    ) -> typing.Mapping[str, typing.Any]:

        if self._value_id_cache is not None:
            return self._value_id_cache

        result = {}
        for root, dirnames, filenames in os.walk(self._metadata_store, topdown=True):

            for filename in [
                f
                for f in filenames
                if os.path.isfile(os.path.join(root, f))
                and f.endswith(".metadata.json")
            ]:

                split = filename[0:-14].split(".", maxsplit=1)
                result[split[0]] = {"type": split[1]}
        self._value_id_cache = result  # type: ignore
        return self._value_id_cache  # type: ignore

    @property
    def value_ids(self) -> typing.Iterable[str]:
        return self.values_metadata.keys()

    @property
    def available_ids(self) -> typing.Set[str]:

        result = set(self.value_ids)
        result.update(self.aliases.keys())
        return result

    @property
    def aliases(self):

        if self._aliases is not None:
            return self._aliases

        result = {}
        for root, dirnames, filenames in os.walk(self._alias_folder, topdown=True):

            for filename in [
                f
                for f in filenames
                if os.path.islink(os.path.join(root, f))
                and f.endswith(".metadata.json")
            ]:

                full_path = os.path.join(root, filename)
                alias = filename[0:-14]
                linked = os.path.realpath(full_path)
                value_id = os.path.basename(linked).split(".")[0]
                result[alias] = value_id
        self._aliases = result  # type: ignore
        return self._aliases

    def get_value_id(self, value_id_or_alias: str):
        if value_id_or_alias in self.aliases.keys():
            return self.aliases[value_id_or_alias]
        elif value_id_or_alias in self.value_ids:
            return value_id_or_alias
        else:
            raise Exception(f"Not a valid value id or alias: {value_id_or_alias}")

    def get_value_type(self, value_id: str) -> str:
        value_id = self.get_value_id(value_id)
        if value_id not in self.values_metadata.keys():
            raise Exception(f"Invalid value id: {value_id}")

        v_type = self.values_metadata[value_id]["type"]
        return v_type

    def get_value_metadata(
        self,
        value_id: str,
        metadata_key: typing.Optional[str] = None,
        also_return_schema: bool = False,
    ) -> typing.Mapping[str, typing.Any]:

        value_id = self.get_value_id(value_id)

        v_type = self.get_value_type(value_id)

        if self.values_metadata[value_id].get("metadata", None) is None:

            path = os.path.join(
                self._metadata_store, f"{value_id}.{v_type}.metadata.json"
            )
            with open(path, "r") as f:
                json_result = json.load(f)
            self.values_metadata[value_id]["metadata"] = json_result

        if also_return_schema:
            if metadata_key:
                result = self.values_metadata[value_id]["metadata"][metadata_key]
            else:
                result = self.values_metadata[value_id]["metadata"]
        else:
            r = {
                k: v["item_metadata"]
                for k, v in self.values_metadata[value_id]["metadata"].items()
            }
            if metadata_key:
                result = r[metadata_key]
            else:
                result = r

        return result

    def get_load_config(self, value_id: str) -> LoadConfig:

        value_id = self.get_value_id(value_id)

        lc = self.get_value_metadata(value_id=value_id, metadata_key="load_config")
        load_config = LoadConfig(**lc)
        return load_config

    def load_value(self, value_id: str) -> "Value":

        value_id = self.get_value_id(value_id)

        load_config = self.get_load_config(value_id=value_id)

        value: Value = self._kiara.run(**load_config.dict())  # type: ignore
        value.id = value_id

        md = self.get_value_metadata(value_id, also_return_schema=True)
        value.metadata = md  # type: ignore
        value.is_set = True
        value.is_none = False
        value.is_constant = True
        return value
