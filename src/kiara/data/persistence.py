# -*- coding: utf-8 -*-
import json
import os
import typing
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field
from tzlocal import get_localzone

from kiara.defaults import KIARA_DATA_STORE, KIARA_METADATA_STORE
from kiara.profiles import ModuleProfileConfig

if typing.TYPE_CHECKING:
    from kiara.data.values import Value
    from kiara.kiara import Kiara


class SaveConfig(ModuleProfileConfig):

    input_name: str = Field(description="The name of the input for the value to save.")
    target_name: str = Field(description="The name of the input to specify the target.")
    load_config_output: str = Field(
        description="The name of the output field that contains the load config."
    )


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


class PersistanceMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._data_store: Path = Path(KIARA_DATA_STORE)
        self._metadata_store: Path = Path(KIARA_METADATA_STORE)
        os.makedirs(self._metadata_store, exist_ok=True)
        self._value_id_cache: typing.Optional[
            typing.Dict[str, typing.Optional[typing.Dict[str, typing.Any]]]
        ] = None

    def save_value(self, value: "Value") -> str:

        value_type = value.type_obj
        _save_config = value_type.save_config()

        if not _save_config:
            raise Exception(f"Saving not supported for type '{value.type_name}'.")

        save_config = SaveConfig(**_save_config)

        module = save_config.create_module(self._kiara)
        constants = module.config.constants
        inputs = dict(constants)
        for k, v in module.config.defaults:
            if k in constants.keys():
                raise Exception(
                    f"Invalid default value '{k}', constant defined for this name."
                )
            inputs[k] = v

        target_path = os.path.join(self._data_store, value.id)
        metadata_path = os.path.join(
            self._metadata_store, f"{value.id}.{value.type_name}.metadata.json"
        )
        if os.path.exists(metadata_path):
            raise Exception(
                f"Can't save value, metadata file alrady exists: {metadata_path}"
            )

        if save_config.input_name in constants.keys():
            raise Exception(
                f"Invalid input field name '{save_config.input_name}', constant defined for this name."
            )
        inputs[save_config.input_name] = value
        if save_config.target_name in constants.keys():
            raise Exception(
                f"Invalid target_name field name '{save_config.input_name}', constant defined for this name."
            )
        inputs[save_config.target_name] = target_path

        result = module.run(**inputs)

        details: Value = result[save_config.load_config_output]

        metadata: typing.Dict[str, typing.Mapping[str, typing.Any]] = dict(
            value.get_metadata(also_return_schema=True)
        )
        assert "snapshot" not in metadata.keys()
        assert "load_config" not in metadata.keys()
        tz = get_localzone()
        local_dt = tz.localize(datetime.now(), is_dst=None)

        ssmd = SnapshotMetadata(
            value_type=value.type_name,
            value_id=value.id,
            value_id_orig=value.id,
            snapshot_time=str(local_dt),
        )
        metadata["snapshot"] = {
            "metadata": ssmd.dict(),
            "metadata_schema": ssmd.schema_json(),
        }
        load_config = details.get_value_data()
        metadata["load_config"] = {
            "metadata": load_config,
            "metadata_schema": LoadConfig.schema_json(),
        }

        with open(metadata_path, "w") as f:
            f.write(json.dumps(metadata))

        # metadata_columns = {
        #     "key": [],
        #     "metadata": [],
        #     "metadata_schema": []
        # }
        #
        # for name, md in metadata.items():
        #     metadata_columns["key"].append(name)
        #     md_json = json.dumps(md["metadata"])
        #     metadata_columns["metadata"].append(md_json)
        #     metadata_columns["metadata_schema"].append(md["metadata_schema"])

        # metadata_tbl = pa.Table.from_pydict(metadata_columns)
        # feather.write_feather(metadata_tbl, metadata_path)

        # load_config_file = os.path.join(target_path,  "load_config.json")
        # with open(load_config_file, 'w') as f:
        #     f.write(json.dumps(load_config))

        # result = {
        #     "target_path": target_path,
        #     "metadata_path": metadata_path,
        #     "metadata": metadata,
        # }

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

                split = filename.split(".")
                result[split[0]] = {"type": split[1]}

        self._value_id_cache = result  # type: ignore
        return self._value_id_cache  # type: ignore

    @property
    def value_ids(self) -> typing.Iterable[str]:
        return self.values_metadata.keys()

    def get_value_type(self, value_id: str) -> str:
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
                k: v["metadata"]
                for k, v in self.values_metadata[value_id]["metadata"].items()
            }
            if metadata_key:
                result = r[metadata_key]
            else:
                result = r

        return result

    def get_load_config(self, value_id: str) -> LoadConfig:

        lc = self.get_value_metadata(value_id=value_id, metadata_key="load_config")
        load_config = LoadConfig(**lc)
        return load_config

    def load_value(self, value_id: str) -> "Value":

        load_config = self.get_load_config(value_id=value_id)

        value: Value = self._kiara.run(**load_config.dict())  # type: ignore

        md = self.get_value_metadata(value_id, also_return_schema=True)
        value.metadata = md  # type: ignore
        value.is_constant = True
        return value
