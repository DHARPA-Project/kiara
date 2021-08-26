# -*- coding: utf-8 -*-
import abc
import logging
import typing
import uuid
from datetime import datetime
from pydantic import BaseModel, Field
from tzlocal import get_localzone

from kiara.metadata.core_models import SnapshotMetadata, ValueHash, ValueInfo
from kiara.metadata.data import LoadConfig, SaveConfig

if typing.TYPE_CHECKING:
    from kiara.data.values import Value
    from kiara.kiara import Kiara
    from kiara.operations.calculate_hash import CalculateHashOperationType

log = logging.getLogger("kiara")


class ValueAlias(BaseModel):
    @classmethod
    def from_string(
        self, value_alias: str, default_repo_name: typing.Optional[str] = None
    ) -> "ValueAlias":

        if not isinstance(value_alias, str):
            raise Exception("Invalid id_or_alias: not a string.")
        if not value_alias:
            raise Exception("Invalid id_or_alias: can't be empty string.")

        _repo_name: typing.Optional[str] = default_repo_name
        _version: typing.Optional[int] = None
        _tag: typing.Optional[str] = None

        if "#" in value_alias:
            _repo_name, _value_alias = value_alias.split("#", maxsplit=1)
        else:
            _value_alias = value_alias

        if "@" in _value_alias:
            _alias, _postfix = _value_alias.split("@", maxsplit=1)

            try:
                _version = int(_postfix)
            except ValueError:
                if not _postfix.isidentifier():
                    raise Exception(
                        f"Invalid format for version/tag element of id_or_alias: {_tag}"
                    )
                _tag = _postfix
        else:
            _alias = _value_alias

        return ValueAlias(
            repo_name=_repo_name, alias=_alias, version=_version, tag=_tag
        )

    @classmethod
    def from_strings(
        cls, *value_aliases: typing.Union[str, "ValueAlias"]
    ) -> typing.List["ValueAlias"]:

        result = []
        for va in value_aliases:
            if isinstance(va, str):
                result.append(ValueAlias.from_string(va))
            elif isinstance(va, ValueAlias):
                result.append(va)
            else:
                raise TypeError(
                    f"Invalid type '{type(va)}' for type alias, expected 'str' or 'ValueAlias'."
                )
        return result

    repo_name: typing.Optional[str] = Field(
        description="The name of the data repo the value lives in.", default=None
    )
    alias: str = Field("The alias name.")
    version: typing.Optional[int] = Field(
        description="The version of this alias.", default=None
    )
    tag: typing.Optional[str] = Field(
        description="The tag for the alias.", default=None
    )

    @property
    def full_alias(self):
        if self.tag is not None:
            return f"{self.alias}@{self.tag}"
        elif self.version is not None:
            return f"{self.alias}@{self.version}"
        else:
            return self.alias


class SavedValueMetadata(BaseModel):

    value_id: str = Field(description="The value id.")
    value_type: str = Field(description="The type of the value.")
    aliases: typing.List[str] = Field(
        description="All aliases for this value.", default_factory=list
    )
    tags: typing.List[str] = Field(
        description="All tags for this value.", default_factory=list
    )
    metadata: typing.Dict[str, typing.Dict[str, typing.Any]] = Field(
        description="The metadata associated with this value."
    )

    def get_metadata_items(self, *keys: str) -> typing.Dict[str, typing.Any]:

        if not keys:
            _keys: typing.Iterable[str] = self.metadata.keys()
        else:
            _keys = keys

        result = {}
        for k in _keys:
            md = self.metadata.get(k)
            if md is None:
                raise Exception(f"No metadata for key '{k}' available.")
            result[k] = md["metadata_item"]

        return result

    def get_metadata_schemas(self, *keys: str) -> typing.Dict[str, typing.Any]:

        if not keys:
            _keys: typing.Iterable[str] = self.metadata.keys()
        else:
            _keys = keys

        result = {}
        for k in _keys:
            md = self.metadata.get(k)
            if md is None:
                raise Exception(f"No metadata for key '{k}' available.")
            result[k] = md["metadata_schema"]

        return result


class DataStore(abc.ABC):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

    @abc.abstractmethod
    def _get_available_value_ids(self) -> typing.Iterable[str]:
        """Return all of the stores available value ids."""

    @abc.abstractmethod
    def _get_available_hashes(self) -> typing.Dict[str, typing.Dict[str, str]]:
        """Return the available hashes of all the values in this store."""

    @abc.abstractmethod
    def _get_available_aliases(self) -> typing.Iterable[str]:
        pass

    @abc.abstractmethod
    def _find_value_id_for_alias_tag(
        self, alias: str, tag: str
    ) -> typing.Optional[str]:
        pass

    @abc.abstractmethod
    def _find_value_id_for_alias_version(
        self, alias: str, version: int
    ) -> typing.Optional[str]:
        pass

    @abc.abstractmethod
    def _find_aliases_for_value_id(self, value_id: str) -> typing.Iterable[ValueAlias]:
        pass

    @abc.abstractmethod
    def _get_versions_for_alias(self, alias: str):
        pass

    @abc.abstractmethod
    def _get_tags_for_alias(self, alias: str):
        pass

    @abc.abstractmethod
    def _get_metadata_for_id(self, value_id: str) -> SavedValueMetadata:
        pass

    # @abc.abstractmethod
    # def get_load_config_for_id(self, value_id: str) -> LoadConfig:
    #     pass

    @abc.abstractmethod
    def _prepare_save_config(
        self, value_id: str, value_type: str, value: "Value"
    ) -> typing.Optional[SaveConfig]:
        """Return the save config for """

    @abc.abstractmethod
    def _register_value(
        self,
        value_id: str,
        value_type: str,
        hashes: typing.Iterable[ValueHash],
        metadata: typing.Mapping[str, typing.Mapping[str, typing.Any]],
    ):
        pass

    @abc.abstractmethod
    def _register_alias(self, value_id: str, alias: str, version: int):
        pass

    @abc.abstractmethod
    def _register_alias_tag(self, value_id: str, alias: str, tag: str):
        pass

    @property
    def value_ids(self) -> typing.Iterable[str]:
        return self._get_available_value_ids()

    @property
    def aliases(self) -> typing.Iterable[str]:
        return self._get_available_aliases()

    @property
    def hashes(self) -> typing.Iterable[str]:
        return self._get_available_hashes()

    def resolve_id(
        self, value_id: str, hash_type: typing.Optional[str] = None
    ) -> typing.Optional[str]:

        if value_id in self.value_ids:
            return value_id

        alias = ValueAlias.from_string(value_id)

        if alias.alias in self.aliases:
            if alias.tag and alias.version:
                raise Exception(
                    f"Invalid alias, setting 'tag' as well as 'version' is not allowed: {value_id}"
                )  # this should never happen

            if alias.tag:
                return self._find_value_id_for_alias_tag(
                    alias=alias.alias, tag=alias.tag
                )
            elif alias.version:
                return self._find_value_id_for_alias_version(
                    alias=alias.alias, version=alias.version
                )
            else:
                latest_version = self.get_latest_version_for_alias(alias=alias.alias)
                return self._find_value_id_for_alias_version(
                    alias=alias.alias, version=latest_version
                )

        v_id = self._find_value_id_for_hash(hash_str=value_id, hash_type=hash_type)
        if v_id:
            return v_id

        return None

    def _find_value_id_for_hash(
        self, hash_str: str, hash_type: typing.Optional[str] = None
    ) -> typing.Optional[str]:

        # this relies that the data store ensures that are no hash collision

        hashes = self._get_available_hashes()
        if hash_type is not None:
            for value_id, _hashes in hashes.items():
                _hash = _hashes.get(hash_type, None)
                if _hash is not None:
                    if _hash == hash_str:
                        return value_id
            return None

        else:
            for value_id, _hashes in hashes.items():
                for _hash_type, _hash in _hashes.items():
                    if _hash == hash_str:
                        return value_id
            return None

    def find_hashes_for_value_id(self, value_id: str) -> typing.Mapping[str, str]:

        return self._get_available_hashes().get(value_id, {})

    def find_aliases_for_value_id(
        self,
        value_id: str,
        include_all_versions: bool = False,
        include_tags: bool = False,
    ) -> typing.Iterable[ValueAlias]:

        aliases = self._find_aliases_for_value_id(value_id=value_id)
        result = []
        latest_cache: typing.Dict[str, int] = {}
        for alias in aliases:

            if alias.version is not None:
                if not include_all_versions:
                    if alias.alias in latest_cache.keys():
                        latest_version = latest_cache[alias.alias]
                    else:
                        latest_version = self.get_latest_version_for_alias(alias.alias)
                        latest_cache[alias.alias] = latest_version
                    if latest_version == alias.version:
                        result.append(alias)
                else:
                    result.append(alias)

            if alias.tag is not None and include_tags:
                result.append(alias)

        return result

    def get_metadata_for_id(self, value_id: str) -> SavedValueMetadata:

        v_id = self.resolve_id(value_id)

        if not v_id:
            raise Exception(f"No value registered for id/alias/hash: {value_id}")

        md = self._get_metadata_for_id(v_id)
        md.aliases = [
            x.full_alias for x in self.find_aliases_for_value_id(value_id=value_id)
        ]
        return md

    def get_value_type_for_id(self, value_id) -> str:

        md = self.get_metadata_for_id(value_id=value_id)
        return md.value_type

    def get_value_type_for_alias(self, alias: str) -> typing.Optional[str]:

        versions = self.get_versions_for_alias(alias)
        if not versions:
            return None

        return self.get_value_type_for_id(f"{alias}@{min(versions)}")

    def get_versions_for_alias(self, alias: str) -> typing.Iterable[int]:

        if isinstance(alias, str):
            _alias = ValueAlias.from_string(alias)
        elif not isinstance(alias, ValueAlias):
            raise TypeError(f"Invalid type for alias: {type(alias)}")
        else:
            _alias = alias

        if _alias.alias not in self.aliases:
            return []

        versions = self._get_versions_for_alias(alias=_alias.alias)
        return sorted(versions)

    def get_tags_for_alias(self, alias: str) -> typing.Iterable[str]:

        if isinstance(alias, str):
            _alias = ValueAlias.from_string(alias)
        elif not isinstance(alias, ValueAlias):
            raise TypeError(f"Invalid type for alias: {type(alias)}")
        else:
            _alias = alias

        if _alias.alias not in self.aliases:
            return []

        tags = self._get_tags_for_alias(alias=_alias.alias)
        return sorted(tags)

    def get_latest_version_for_alias(self, alias: str) -> int:

        versions = self.get_versions_for_alias(alias)
        if not versions:
            return 0
        else:
            return max(versions)

    def register_alias(self, value_id: str, alias: typing.Union[str, ValueAlias]):

        if isinstance(alias, str):
            alias = ValueAlias.from_string(alias)

        if alias.version is not None:
            raise Exception(
                "Can't register specific alias version, prefix must be a tag or empty."
            )

        tag_exists = False
        if alias.tag:
            _v_id = self._find_value_id_for_alias_tag(alias=alias.alias, tag=alias.tag)
            if _v_id:
                if value_id != _v_id:
                    tag_exists = True
                else:
                    raise Exception(
                        f"Tag '{alias.tag}' already exists for alias '{alias.alias}'."
                    )

        version = self.get_latest_version_for_alias(alias.alias)

        latest_version_id = self.resolve_id(f"{alias.alias}@{version}")
        if latest_version_id == value_id:
            log.debug(
                f"Latest version of '{alias.alias}' already points to the right value id: ({value_id}), not registering new alias version..."
            )
        else:
            self._register_alias(
                value_id=value_id, alias=alias.alias, version=version + 1
            )

        if alias.tag and not tag_exists:
            self._register_alias_tag(
                value_id=value_id, alias=alias.alias, tag=alias.tag
            )

    def register_aliases(self, value_id: str, *aliases: typing.Union[str, ValueAlias]):
        for alias in aliases:
            self.register_alias(value_id=value_id, alias=alias)

    def save_value(
        self,
        value: "Value",
        aliases: typing.Optional[typing.Iterable[typing.Union[str, ValueAlias]]] = None,
        value_type: typing.Optional[str] = None,
    ) -> SavedValueMetadata:

        if value_type is None:
            value_type = value.type_name
        else:
            if value_type != "all" and value_type != value.type_name:
                raise Exception(
                    f"Can't save value: mismatching value types '{value_type} (expected)' !=  '{value.type_name} (actual)'"
                )
        # TODO: validate type if specfied in the kwargs?

        # check this before any real work is done -- fail early
        if aliases is None:
            aliases = []
        value_aliases = []
        for va in aliases:
            if isinstance(va, str):
                va_obj = ValueAlias.from_string(va)
                if va_obj.version is not None:
                    raise Exception(
                        f"Can't register specific alias version, prefix must be a tag or empty: {va}"
                    )
                value_aliases.append(va_obj)
            else:
                if va.version is not None:
                    raise Exception(
                        f"Can't register specific alias version, prefix must be a tag or empty: {va.full_alias}"
                    )
                value_aliases.append(va)

        for va in value_aliases:
            if va.tag:
                _v_id = self._find_value_id_for_alias_tag(alias=va.alias, tag=va.tag)
                if _v_id:
                    raise Exception(
                        f"Not saving value: tag '{va.tag}' already exists for alias '{va.alias}'."
                    )
            alias_vt = self.get_value_type_for_alias(alias=va.alias)
            if alias_vt and alias_vt != value_type:
                raise Exception(
                    f"Not saving value: value type '{value_type}' different to existing alias value type '{alias_vt}'."
                )

        # try to calculate the hash for the value, so we can use it as value id
        hash_ops: CalculateHashOperationType = self._kiara.operation_mgmt.get_operations("calculate_hash")  # type: ignore

        hash_ops_for_type = hash_ops.get_hash_operations_for_type(value_type=value_type)

        hashes = {}
        for op_id, op_config in hash_ops_for_type.items():

            op_module = op_config.module

            result = op_module.run(value_item=value)

            _hash = result.get_value_data("hash")
            hashes[op_id] = ValueHash(hash=_hash, hash_type=op_id)

        new_value_id = None
        existing = False
        for value_hash in hashes.values():
            existing_value = self._find_value_id_for_hash(
                hash_str=value_hash.hash, hash_type=value_hash.hash_type
            )
            if existing_value is not None:
                new_value_id = existing_value
                existing = True
                break

        if not existing:
            new_value_id = str(uuid.uuid4())
            log.debug(
                f"Did not find existing value, saving new value with id: {new_value_id}"
            )
        else:
            log.debug(f"Found existing value, not saving again: {new_value_id}")

        assert new_value_id is not None

        if new_value_id not in self._get_available_value_ids():
            # a value item with this id was not already stored

            # run the type and repo type specific save module, and retrieve the load_config that is needed to retrieve it again later
            save_config = self._prepare_save_config(
                value_id=new_value_id, value_type=value_type, value=value
            )

            if save_config is None:
                raise Exception(
                    f"Can't save value: no save operation found for value type '{value_type}'"
                )

            save_module = save_config.create_module(kiara=self._kiara)
            result = save_module.run(**save_config.inputs)
            load_config_value: Value = result.get_value_obj("load_config")

            # assemble the value metadata
            # TODO: do this in parallel with the 'saving' process
            metadata: typing.Dict[str, typing.Mapping[str, typing.Any]] = dict(
                value.get_metadata(also_return_schema=True)
            )
            assert "snapshot" not in metadata.keys()
            assert "load_config" not in metadata.keys()
            assert "value" not in metadata.keys()
            assert "value_lineage" not in metadata.keys()

            tz = get_localzone()
            local_dt = tz.localize(datetime.now(), is_dst=None)

            ssmd = SnapshotMetadata(
                value_type=value_type,
                value_id=new_value_id,
                value_id_orig=value.id,
                snapshot_time=str(local_dt),
            )
            metadata["value"] = {
                "metadata_item": ValueInfo(type=value.type_name, hashes=hashes).dict(),
                "metadata_item_schema": ValueInfo.schema_json(),
            }
            metadata["snapshot"] = {
                "metadata_item": ssmd.dict(),
                "metadata_item_schema": ssmd.schema_json(),
            }
            load_config = load_config_value.get_value_data()
            metadata["load_config"] = {
                "metadata_item": load_config.dict(),
                "metadata_item_schema": LoadConfig.schema_json(),
            }
            # metadata["value_lineage"] = {
            #     "metadata_item": value.value_metadata.dict(),
            #     "metadata_item_schema": value.value_metadata.schema_json(),
            # }

            self._register_value(
                value_id=new_value_id,
                value_type=value_type,
                hashes=hashes.values(),
                metadata=metadata,
            )

        if value_aliases:
            self.register_aliases(new_value_id, *value_aliases)

        return self.get_metadata_for_id(new_value_id)

    def load_value(self, value_id: str) -> "Value":

        v_id = self.resolve_id(value_id)
        if v_id is None:
            raise Exception(f"Can't load value: no value registered for '{value_id}'")

        md = self.get_metadata_for_id(v_id)
        load_config_dict = md.metadata["load_config"]

        load_config = LoadConfig(**load_config_dict["metadata_item"])
        value: Value = self._kiara.run(**load_config.dict(exclude={"value_id", "base_path_input_name", "doc"}))  # type: ignore
        value.id = value_id

        value.metadata = md.metadata
        value.is_set = True
        value.is_none = False
        value.is_constant = True
        return value

    def find_all_values_of_type(
        self, type: str
    ) -> typing.Dict[str, SavedValueMetadata]:

        result = {}
        for value_id in self.value_ids:
            vmd = self.get_metadata_for_id(value_id)
            if vmd.value_type == type:
                result[value_id] = vmd

        return result
