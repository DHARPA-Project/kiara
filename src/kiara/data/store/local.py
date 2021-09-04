# -*- coding: utf-8 -*-
import json
import os
import typing
from pathlib import Path
from pydantic import Field

from kiara.data import Value
from kiara.data.registry import DataRegistry
from kiara.data.values import ValueInfo, ValueSchema, ValueSlot
from kiara.defaults import SpecialValue
from kiara.metadata.data import LoadConfig, SaveConfig
from kiara.utils import log_message

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.operations.save_value import SaveOperationType


class SavedValueInfo(ValueInfo):

    load_config: LoadConfig = Field(
        description="The configuration to load this value from disk (or however it is stored)."
    )


class LocalDataStore(DataRegistry):
    def __init__(self, kiara: "Kiara", base_path: typing.Union[str, Path]):

        if isinstance(base_path, str):
            base_path = Path(base_path)

        self._base_path: Path = base_path
        self._value_obj_cache: typing.Dict[str, Value] = {}
        self._data_cache: typing.Dict[str, Value] = {}
        self._metadata_cache: typing.Dict[str, typing.Any] = {}
        self._value_slots: typing.Dict[str, ValueSlot] = {}
        super().__init__(kiara=kiara)

    def _get_value_obj_for_id(self, value_id: str) -> Value:

        if value_id in self._value_obj_cache.keys():
            return self._value_obj_cache[value_id]

        metadata_path = self.get_metadata_path(value_id=value_id)
        if not metadata_path.exists():
            raise Exception(f"No value with id '{value_id}' registered.")

        info_content = metadata_path.read_text()
        info_dict = json.loads(info_content)
        value_info = SavedValueInfo(**info_dict)

        assert value_info.value_id == value_id

        value_schema = ValueSchema(
            type=value_info.value_type, type_config=value_info.value_type_config
        )

        value_obj = self._create_value_obj(
            value_schema=value_schema,
            is_set=True,
            is_none=False,
            value_hashes=value_info.hashes,
            metadata=value_info.metadata,
        )
        value_obj.id = value_info.value_id
        self._register_new_value_obj(
            value_obj=value_obj,
            value_data=SpecialValue.IGNORE,
            value_seed=None,
            value_hashes=value_info.hashes,
        )
        self._value_obj_cache[value_obj.id] = value_obj
        return value_obj

    def _get_value_data_for_id(self, value_id: str) -> typing.Any:

        if value_id in self._data_cache.keys():
            return self._data_cache[value_id].get_value_data()

        # value_obj = self.get_value_obj(value_item=value_id)
        metadata_path = self.get_metadata_path(value_id=value_id)
        if not metadata_path.exists():
            raise Exception(f"No value with id '{value_id}' registered.")

        info_content = metadata_path.read_text()
        info_dict = json.loads(info_content)
        value_info = SavedValueInfo(**info_dict)

        assert value_info.value_id == value_id

        load_config = value_info.load_config
        value: Value = self._kiara.run(**load_config.dict(exclude={"value_id", "base_path_input_name", "doc"}))  # type: ignore
        self._data_cache[value_id] = value
        return value.get_value_data()

    def _register_value_and_data(self, value: Value, data: typing.Any) -> str:

        if data == SpecialValue.IGNORE:
            return value.id

        raise NotImplementedError()
        #
        # new_value_id = str(uuid.uuid4())
        #
        # value.id = new_value_id
        # value_type = value.type_name
        #
        # # run the type and repo type specific save module, and retrieve the load_config that is needed to retrieve it again later
        # save_config = self._prepare_save_config(
        #     value_id=new_value_id, value_type=value_type, value=value
        # )
        #
        # save_module = save_config.create_module(kiara=self._kiara)
        # result = save_module.run(**save_config.inputs)
        # load_config_value: Value = result.get_value_obj("load_config")
        #
        # # assemble the value metadata
        # metadata: typing.Dict[str, typing.Mapping[str, typing.Any]] = dict(
        #     value.get_metadata(also_return_schema=True)
        # )

    def _register_remote_value(self, value: Value) -> typing.Optional[Value]:

        value_metadata_path = self.get_metadata_path(value.id)
        if value_metadata_path.exists():
            raise Exception(
                f"Can't register remote value with id '{value.id}'. Load config for this id already exists: {value_metadata_path.as_posix()}"
            )

        value_type = value.type_name

        # run the type and repo type specific save module, and retrieve the load_config that is needed to retrieve it again later
        save_config = self._prepare_save_config(
            value_id=value.id, value_type=value_type, value=value
        )

        save_module = save_config.create_module(kiara=self._kiara)
        result = save_module.run(**save_config.inputs)
        load_config_value: Value = result.get_value_obj("load_config")

        load_config_data = load_config_value.get_value_data()

        value_info = SavedValueInfo(
            value_id=value.id,
            value_type=value.type_name,
            value_type_config=value.type_obj.type_config,
            hashes=value.get_hashes(),
            metadata=value.get_metadata(also_return_schema=True),
            load_config=load_config_data,
        )

        value_metadata_path.write_text(json.dumps(value_info.dict()))

        copied_value = value.copy()
        copied_value._registry = self
        copied_value._kiara = self._kiara

        self._value_obj_cache[copied_value.id] = copied_value

        return copied_value

    def _prepare_save_config(
        self, value_id: str, value_type: str, value: "Value"
    ) -> SaveConfig:

        save_operations: SaveOperationType = self._kiara.operation_mgmt.get_operations("save_value")  # type: ignore

        op_config = save_operations.get_save_operation_for_type(value_type)

        if not op_config:
            raise Exception(
                f"Can't save value: no save operation found for value type '{value_type}'"
            )

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

    def get_data_path(self, value_id: str) -> Path:
        return self._base_path / f"value_{value_id}" / "data"

    def get_metadata_path(self, value_id: str) -> Path:
        path = self._base_path / f"value_{value_id}" / "value_metadata.json"
        return path

    # def get_load_config_path(self, value_id: str) -> Path:
    #     path = self._base_path / f"value_{value_id}" / "load_config.json"
    #     return path

    def _get_value_slot_for_alias(self, alias_name: str) -> ValueSlot:

        if alias_name in self._value_slots.keys():
            return self._value_slots[alias_name]

        alias_file = self._base_path / "aliases" / f"alias_{alias_name}.json"
        if not alias_file.is_file():
            raise Exception(f"No alias with name '{alias_name}' registered.")

        file_content = alias_file.read_text()
        alias_data = json.loads(file_content)
        value_schema = ValueSchema(**alias_data["value_schema"])

        value_slot = ValueSlot(
            id=alias_name, value_schema=value_schema, kiara=self._kiara, registry=self
        )

        for version in sorted(alias_data["versions"].keys()):
            value_id = alias_data["versions"][version]
            value_obj = self.get_value_obj(value_item=value_id)
            if value_obj is None:
                raise Exception(f"Can't find value with id: {value_id}")
            tags: typing.Optional[typing.Iterable[str]] = None
            if value_id in alias_data["tags"].values():
                tags = [
                    tag
                    for tag, version in alias_data["tags"].items()
                    if version == version
                ]

            new_version = value_slot.add_value(
                value_obj, trigger_callbacks=False, tags=tags
            )
            if new_version != int(version):
                raise Exception(
                    f"New version different to stored version ({new_version} != {version}). This is a bug."
                )

        self._value_slots[alias_name] = value_slot

        return self._value_slots[alias_name]

    def _get_available_aliases(self) -> typing.Iterable[str]:

        alias_path = self._base_path / "aliases"
        alias_file = alias_path.glob("alias_*.json")
        result = []
        for af in alias_file:
            if not af.is_file():
                log_message(f"Ignoring non-file: {af.as_posix()}")
                continue
            alias_name = af.name[6:-5]
            result.append(alias_name)
        return result

    def _get_available_value_ids(self) -> typing.Iterable[str]:
        value_ids = [
            x.parent.name[6:]
            for x in self._base_path.glob("value_*/value_metadata.json")
        ]

        return value_ids

    def _register_alias(self, alias_name: str, value_schema: ValueSchema) -> ValueSlot:

        alias_file = self._base_path / "aliases" / f"alias_{alias_name}.json"
        if alias_file.exists():
            raise Exception(f"Alias '{alias_name}' already registered.")

        if alias_name in self._value_slots.keys():
            raise Exception(f"Value slot for alias '{alias_name}' already exists.")

        alias_file.parent.mkdir(parents=True, exist_ok=True)

        vs = ValueSlot(
            id=alias_name, value_schema=value_schema, kiara=self._kiara, registry=self
        )
        self._value_slots[alias_name] = vs

        self._write_alias_file(alias_name)
        vs.register_callbacks(self)

        return vs

    def _write_alias_file(self, alias_name):

        alias_file = self._base_path / "aliases" / f"alias_{alias_name}.json"

        value_slot = self._value_slots[alias_name]

        alias_dict = {
            "value_schema": value_slot.value_schema.dict(),
            "versions": {},
            "tags": {},
        }

        for version, value in value_slot.values.items():
            alias_dict["versions"][version] = value.id

        for tag_name, version in value_slot.tags.items():
            alias_dict["tags"][tag_name] = version

        alias_file.write_text(json.dumps(alias_dict))

    def values_updated(self, *items: "ValueSlot"):

        invalid = []
        for item in items:
            if item.id not in self._value_slots.keys():
                invalid.append(item.id)

        if invalid:
            raise Exception(
                f"Can't update value(s), invalid aliases: {', '.join(invalid)}"
            )

        for item in items:
            self._write_alias_file(alias_name=item.id)


# class LocalDataStore(DataStore):
#
#     def __init__(self, kiara: "Kiara", base_path: typing.Union[str, Path]):
#
#         if isinstance(base_path, str):
#             base_path = Path(base_path)
#
#         self._base_path: Path = base_path
#         self._metadata_cache: typing.Dict[str, typing.Any] = {}
#         super().__init__(kiara=kiara)
#
#
#     def _get_available_hashes(self) -> typing.Dict[str, typing.Dict[str, str]]:
#
#         hash_files = self._base_path.glob("value_*/hash_*.json")
#         result: typing.Dict[str, typing.Dict[str, str]] = {}
#         for hf in hash_files:
#             value_id = hf.parent.name[6:]
#             hash_type, hash_value = hf.name.split(".", maxsplit=1)
#             hash_type = hash_type[5:]
#             hash_value = hash_value[0:-5]
#             result.setdefault(value_id, {})[hash_type] = hash_value
#
#         return result
#
#     def _prepare_save_config(
#         self, value_id: str, value_type: str, value: "Value"
#     ) -> typing.Optional[SaveConfig]:
#
#         save_operations: SaveOperationType = self._kiara.operation_mgmt.get_operations("save_value")  # type: ignore
#
#         op_config = save_operations.get_save_operation_for_type(value_type)
#
#         if not op_config:
#             return None
#
#         target_path = self.get_data_path(value_id=value_id)
#         metadata_path = self.get_metadata_path(value_id=value_id)
#
#         if os.path.exists(metadata_path):
#             raise Exception(
#                 f"Can't save value, metadata file already exists: {metadata_path}"
#             )
#
#         inputs = {
#             "value_item": value.get_value_data(),
#             "base_path": target_path.as_posix(),
#             "value_id": value_id,
#         }  # type: ignore
#
#         save_config = SaveConfig(
#             module_type=op_config.module_type,
#             module_config=op_config.module_config,
#             inputs=inputs,
#             load_config_output="load_config",
#         )
#
#         return save_config
#
#     def _register_saved_value(self, value_id: str, value_type: str, hashes: typing.Optional[typing.Iterable[ValueHash]], metadata: typing.Mapping[str, typing.Any]):
#         pass


# def get_data_path(self, value_id: str) -> Path:
#     return self._base_path / f"value_{value_id}" / "data"
#
# def get_metadata_path(self, value_id: str) -> Path:
#     path = self._base_path / f"value_{value_id}" / "value_metadata.json"
#     return path

# def _get_available_value_ids(self) -> typing.Iterable[str]:
#
#     value_ids = [
#         x.parent.name[6:]
#         for x in self._base_path.glob("value_*/value_metadata.json")
#     ]
#
#     return value_ids
#
# def _get_available_aliases(self) -> typing.Iterable[str]:
#
#     alias_files = self._base_path.glob("value_*/alias_*.version_*.json")
#     result = set()
#     for af in alias_files:
#         version_idx = af.name.rfind(".version_")
#         alias = af.name[6:version_idx]
#         result.add(alias)
#     return result
#
# def _get_versions_for_alias(self, alias: str) -> typing.Iterable[int]:
#
#     alias_files = self._base_path.glob(f"value_*/alias_{alias}.version_*.json")
#     result = []
#     for af in alias_files:
#         idx = af.name.rfind(".version_") + 9
#         substr = af.name[idx:-5]
#         result.append(int(substr))
#     return result
#
# def _get_tags_for_alias(self, alias: str) -> typing.Iterable[str]:
#
#     tag_files = self._base_path.glob(f"value_*/alias_{alias}.tag_*.json")
#     result = []
#     for af in tag_files:
#         idx = af.name.rfind(".tag_") + 5
#         substr = af.name[idx:-5]
#         result.append(substr)
#     return result
#
# def _find_aliases_for_value_id(self, value_id: str) -> typing.Iterable[ValueAlias]:
#
#     result = []
#     alias_files = self._base_path.glob(f"value_{value_id}/alias_*.version_*.json")
#     for af in alias_files:
#         version_idx = af.name.rfind(".version_")
#         version_str = af.name[version_idx + 9 : -5]  # noqa
#         alias = af.name[6:version_idx]
#         va = ValueAlias(alias=alias, version=int(version_str))
#         result.append(va)
#
#     tag_files = self._base_path.glob("value_*/alias_*.tag_*.json")
#     for tf in tag_files:
#         tag_idx = tf.name.rfind(".tag_")
#         tag_str = tf.name[tag_idx + 5 : -5]  # noqa
#         alias = tf.name[6:tag_idx]
#         va = ValueAlias(alias=alias, tag=tag_str)
#         result.append(va)
#     return result
#
# def _find_value_id_for_alias_version(
#     self, alias: str, version: int
# ) -> typing.Optional[str]:
#
#     alias_files = list(
#         self._base_path.glob(f"value_*/alias_{alias}.version_{version}.json")
#     )
#     if not alias_files:
#         return None
#     if len(alias_files) != 1:
#         raise Exception(
#             f"Found more than one value for alias '{alias}' and version '{version}'. This is a bug."
#         )
#
#     return alias_files[0].parent.name[6:]
#
# def _find_value_id_for_alias_tag(
#     self, alias: str, tag: str
# ) -> typing.Optional[str]:
#
#     tag_files = list(self._base_path.glob(f"value_*/alias_{alias}.tag_{tag}.json"))
#     if not tag_files:
#         return None
#     if len(tag_files) != 1:
#         raise Exception(
#             f"Found more than one value for alias '{alias}' and tag '{tag}'. This is a bug."
#         )
#
#     return tag_files[0].parent.name[6:]
#
# def _get_metadata_for_id(self, value_id: str) -> SavedValueMetadata:
#
#     if value_id in self._metadata_cache.keys():
#         return self._metadata_cache[value_id]
#     else:
#         path = self.get_metadata_path(value_id=value_id)
#         md = json.loads(path.read_text())
#
#     value_type = md["value"]["metadata_item"]["type"]
#
#     vmd = SavedValueMetadata(value_id=value_id, value_type=value_type, metadata=md)
#     self._metadata_cache[value_id] = vmd
#     return vmd
#
# def _register_value(
#     self,
#     value_id: str,
#     value_type: str,
#     hashes: typing.Iterable[ValueHash],
#     metadata: typing.Mapping[str, typing.Mapping[str, typing.Any]],
# ) -> None:
#
#     metadata_path = self.get_metadata_path(value_id)
#     metadata_path.parent.mkdir(parents=True, exist_ok=True)
#
#     hashes_files = []
#     for value_hash in hashes:
#         path = (
#             metadata_path.parent
#             / f"hash_{value_hash.hash_type}.{value_hash.hash}.json"
#         )
#         assert not path.exists()
#         hashes_files.append(path)
#
#     with open(metadata_path, "w") as _f:
#         _f.write(json.dumps(metadata))
#
#     for hf in hashes_files:
#         with open(hf, "a"):
#             os.utime(hf, None)
#         # os.symlink(metadata_path.name, hf)
#
#     vmd = SavedValueMetadata(
#         value_id=value_id, value_type=value_type, metadata=metadata
#     )
#     self._metadata_cache[value_id] = vmd
#
# def _register_alias(self, value_id: str, alias: str, version: int):
#
#     metadata_path = self.get_metadata_path(value_id)
#     alias_version_path = (
#         metadata_path.parent / f"alias_{alias}.version_{version}.json"
#     )
#     with open(alias_version_path, "a"):
#         os.utime(alias_version_path, None)
#
#     # os.symlink(metadata_path.name, alias_version_path)
#
# def _register_alias_tag(self, value_id: str, alias: str, tag: str):
#
#     metadata_path = self.get_metadata_path(value_id=value_id)
#     tag_path = metadata_path.parent / f"alias_{alias}.tag_{tag}.json"
#     with open(tag_path, "a"):
#         os.utime(tag_path, None)
#     # os.symlink(metadata_path.name, tag_path)
