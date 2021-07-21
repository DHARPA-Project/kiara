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
    NO_HASH_MARKER,
)

if typing.TYPE_CHECKING:
    pass

    from kiara.data.operations.save_value import SaveOperationType
    from kiara.data.values import Value
    from kiara.kiara import Kiara


class SnapshotMetadata(BaseModel):

    value_type: str = Field(description="The value type.")
    value_id: str = Field(description="The value id after the snapshot.")
    value_id_orig: str = Field(description="The value id before the snapshot.")
    snapshot_time: str = Field(description="The time the data was saved.")


class LoadConfig(ModuleProfileConfig):

    value_id: str = Field(description="The id of the value.")
    base_path_input_name: str = Field(
        description="The base path where the value is stored.", default="base_path"
    )
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
        self._last_metadata_change: float = 0
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

    def save_value(
        self,
        value: "Value",
        aliases: typing.Optional[typing.Iterable[str]] = None,
        overwrite_aliases: bool = True,
        value_type: typing.Optional[str] = None,
    ) -> str:

        if aliases is None:
            aliases = []

        if value_type is None:
            value_type = value.type_name

        # TODO: validate type if specfied in the kwargs?

        op_config: SaveOperationType = self._kiara.data_operations.get_operation(  # type: ignore
            value_type=value_type,
            operation_name="save_value",
            operation_id="data_store",
        )

        if op_config is None:
            raise Exception(
                f"Can't save value: no 'save_value.data_store' operation registered for value type: {value_type}"
            )

        op = self._kiara.data_operations.get_operation(
            value_type=value_type,
            operation_name="calculate_hash",
            operation_id="default",
            raise_exception=False,
        )
        if not op:
            new_value_id: str = str(uuid.uuid4())
        else:
            result = self._kiara.data_operations.run(
                operation_name="calculate_hash",
                operation_id="default",
                value=value,
                value_type=value_type,
            )
            _hash = result.get_value_data("metadata_item")["hash"]
            if _hash == NO_HASH_MARKER:
                new_value_id = str(uuid.uuid4())
            else:
                new_value_id = _hash

        if new_value_id in self.value_ids:
            # a value item with this hash was already stored, we don't need to do it again
            return new_value_id

        if not overwrite_aliases:
            invalid = self.check_existing_aliases(*aliases)

            if invalid:
                raise Exception(
                    f"Can't save value, 'overwrite_aliases' turned off, and alias(es) already registered: {', '.join(invalid)}"
                )

        target_path = os.path.join(self._data_store, new_value_id)
        metadata_path = os.path.join(
            self._metadata_store, f"{new_value_id}.{value_type}.metadata.json"
        )
        if os.path.exists(metadata_path):
            raise Exception(
                f"Can't save value, metadata file alrady exists: {metadata_path}"
            )

        other_inputs = {
            "base_path": target_path,
            "value_id": new_value_id,
        }  # type: ignore
        result = self._kiara.data_operations.run(
            operation_name="save_value",
            operation_id="data_store",
            value=value,
            other_inputs=other_inputs,
            value_type=value_type,
        )

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
            value_type=value_type,
            value_id=new_value_id,
            value_id_orig=value.id,
            snapshot_time=str(local_dt),
        )
        metadata["snapshot"] = {
            "metadata_item": ssmd.dict(),
            "metadata_item_schema": ssmd.schema_json(),
        }
        load_config = load_config_value.get_value_data()
        metadata["load_config"] = {
            "metadata_item": load_config.dict(),
            "metadata_item_schema": LoadConfig.schema_json(),
        }
        metadata["value"] = {
            "metadata_item": value.value_metadata.dict(),
            "metadata_item_schema": value.value_metadata.schema_json(),
        }

        with open(metadata_path, "w") as _f:
            _f.write(json.dumps(metadata))

        for alias in aliases:
            alias_file = os.path.join(self._alias_folder, f"{alias}.metadata.json")
            os.symlink(metadata_path, alias_file)

        return ssmd.value_id

    @property
    def values_metadata(
        self,
    ) -> typing.Mapping[str, typing.Any]:

        if self._value_id_cache is not None:
            last_mod_time = os.path.getmtime(self._metadata_store)
            if self._last_metadata_change == last_mod_time:
                return self._value_id_cache
            else:
                self._value_id_cache = None

        # TODO: lots of potential to make this faster, as it is it re-reads all the metadata file anew
        # after cache clearing. Not worth it optimizing just yet.

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
        self._last_metadata_change = os.path.getmtime(self._metadata_store)

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
                k: v["metadata_item"]
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
        value: Value = self._kiara.run(**load_config.dict(exclude={"value_id", "base_path_input_name"}))  # type: ignore
        value.id = value_id

        md = self.get_value_metadata(value_id, also_return_schema=True)
        value.metadata = md  # type: ignore
        value.is_set = True
        value.is_none = False
        value.is_constant = True
        return value
