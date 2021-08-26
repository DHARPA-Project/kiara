# -*- coding: utf-8 -*-
import json
import os
import typing
from pathlib import Path

from kiara.data.store import DataStore, SavedValueMetadata, ValueAlias
from kiara.metadata.core_models import ValueHash
from kiara.metadata.data import SaveConfig

if typing.TYPE_CHECKING:
    from kiara.data.values import Value
    from kiara.kiara import Kiara
    from kiara.operations.save_value import SaveOperationType


class LocalDataStore(DataStore):
    def __init__(self, kiara: "Kiara", base_path: typing.Union[str, Path]):

        if isinstance(base_path, str):
            base_path = Path(base_path)

        self._base_path: Path = base_path
        self._metadata_cache: typing.Dict[str, typing.Any] = {}
        super().__init__(kiara=kiara)

    def get_data_path(self, value_id: str) -> Path:
        return self._base_path / f"value_{value_id}" / "data"

    def get_metadata_path(self, value_id: str) -> Path:
        path = self._base_path / f"value_{value_id}" / "value_metadata.json"
        return path

    def _get_available_hashes(self) -> typing.Dict[str, typing.Dict[str, str]]:

        hash_files = self._base_path.glob("value_*/hash_*.json")
        result: typing.Dict[str, typing.Dict[str, str]] = {}
        for hf in hash_files:
            value_id = hf.parent.name[6:]
            hash_type, hash_value = hf.name.split(".", maxsplit=1)
            hash_type = hash_type[5:]
            hash_value = hash_value[0:-5]
            result.setdefault(value_id, {})[hash_type] = hash_value

        return result

    def _get_available_value_ids(self) -> typing.Iterable[str]:

        value_ids = [
            x.parent.name[6:]
            for x in self._base_path.glob("value_*/value_metadata.json")
        ]

        return value_ids

    def _get_available_aliases(self) -> typing.Iterable[str]:

        alias_files = self._base_path.glob("value_*/alias_*.version_*.json")
        result = set()
        for af in alias_files:
            version_idx = af.name.rfind(".version_")
            alias = af.name[6:version_idx]
            result.add(alias)
        return result

    def _get_versions_for_alias(self, alias: str) -> typing.Iterable[int]:

        alias_files = self._base_path.glob(f"value_*/alias_{alias}.version_*.json")
        result = []
        for af in alias_files:
            idx = af.name.rfind(".version_") + 9
            substr = af.name[idx:-5]
            result.append(int(substr))
        return result

    def _get_tags_for_alias(self, alias: str) -> typing.Iterable[str]:

        tag_files = self._base_path.glob(f"value_*/alias_{alias}.tag_*.json")
        result = []
        for af in tag_files:
            idx = af.name.rfind(".tag_") + 5
            substr = af.name[idx:-5]
            result.append(substr)
        return result

    def _find_aliases_for_value_id(self, value_id: str) -> typing.Iterable[ValueAlias]:

        result = []
        alias_files = self._base_path.glob(f"value_{value_id}/alias_*.version_*.json")
        for af in alias_files:
            version_idx = af.name.rfind(".version_")
            version_str = af.name[version_idx + 9 : -5]  # noqa
            alias = af.name[6:version_idx]
            va = ValueAlias(alias=alias, version=int(version_str))
            result.append(va)

        tag_files = self._base_path.glob("value_*/alias_*.tag_*.json")
        for tf in tag_files:
            tag_idx = tf.name.rfind(".tag_")
            tag_str = tf.name[tag_idx + 5 : -5]  # noqa
            alias = tf.name[6:tag_idx]
            va = ValueAlias(alias=alias, tag=tag_str)
            result.append(va)
        return result

    def _find_value_id_for_alias_version(
        self, alias: str, version: int
    ) -> typing.Optional[str]:

        alias_files = list(
            self._base_path.glob(f"value_*/alias_{alias}.version_{version}.json")
        )
        if not alias_files:
            return None
        if len(alias_files) != 1:
            raise Exception(
                f"Found more than one value for alias '{alias}' and version '{version}'. This is a bug."
            )

        return alias_files[0].parent.name[6:]

    def _find_value_id_for_alias_tag(
        self, alias: str, tag: str
    ) -> typing.Optional[str]:

        tag_files = list(self._base_path.glob(f"value_*/alias_{alias}.tag_{tag}.json"))
        if not tag_files:
            return None
        if len(tag_files) != 1:
            raise Exception(
                f"Found more than one value for alias '{alias}' and tag '{tag}'. This is a bug."
            )

        return tag_files[0].parent.name[6:]

    def _get_metadata_for_id(self, value_id: str) -> SavedValueMetadata:

        if value_id in self._metadata_cache.keys():
            return self._metadata_cache[value_id]
        else:
            path = self.get_metadata_path(value_id=value_id)
            md = json.loads(path.read_text())

        value_type = md["value"]["metadata_item"]["type"]

        vmd = SavedValueMetadata(value_id=value_id, value_type=value_type, metadata=md)
        self._metadata_cache[value_id] = vmd
        return vmd

    def _prepare_save_config(
        self, value_id: str, value_type: str, value: "Value"
    ) -> typing.Optional[SaveConfig]:

        save_operations: SaveOperationType = self._kiara.operation_mgmt.get_operations("save_value")  # type: ignore

        op_config = save_operations.get_save_operation_for_type(value_type)

        if not op_config:
            return None

        target_path = self.get_data_path(value_id=value_id)
        metadata_path = self.get_metadata_path(value_id=value_id)

        if os.path.exists(metadata_path):
            raise Exception(
                f"Can't save value, metadata file already exists: {metadata_path}"
            )

        inputs = {
            "value_item": value.get_value_data(),
            "base_path": target_path.as_posix(),
            "value_id": value_id,
        }  # type: ignore

        save_config = SaveConfig(
            module_type=op_config.module_type,
            module_config=op_config.module_config,
            inputs=inputs,
            load_config_output="load_config",
        )

        return save_config

    def _register_value(
        self,
        value_id: str,
        value_type: str,
        hashes: typing.Iterable[ValueHash],
        metadata: typing.Mapping[str, typing.Mapping[str, typing.Any]],
    ) -> None:

        metadata_path = self.get_metadata_path(value_id)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)

        hashes_files = []
        for value_hash in hashes:
            path = (
                metadata_path.parent
                / f"hash_{value_hash.hash_type}.{value_hash.hash}.json"
            )
            assert not path.exists()
            hashes_files.append(path)

        with open(metadata_path, "w") as _f:
            _f.write(json.dumps(metadata))

        for hf in hashes_files:
            with open(hf, "a"):
                os.utime(hf, None)
            # os.symlink(metadata_path.name, hf)

        vmd = SavedValueMetadata(
            value_id=value_id, value_type=value_type, metadata=metadata
        )
        self._metadata_cache[value_id] = vmd

    def _register_alias(self, value_id: str, alias: str, version: int):

        metadata_path = self.get_metadata_path(value_id)
        alias_version_path = (
            metadata_path.parent / f"alias_{alias}.version_{version}.json"
        )
        with open(alias_version_path, "a"):
            os.utime(alias_version_path, None)

        # os.symlink(metadata_path.name, alias_version_path)

    def _register_alias_tag(self, value_id: str, alias: str, tag: str):

        metadata_path = self.get_metadata_path(value_id=value_id)
        tag_path = metadata_path.parent / f"alias_{alias}.tag_{tag}.json"
        with open(tag_path, "a"):
            os.utime(tag_path, None)
        # os.symlink(metadata_path.name, tag_path)
