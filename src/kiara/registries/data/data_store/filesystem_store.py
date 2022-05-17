# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import orjson
import shutil
import structlog
import uuid
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Set, Union

from kiara.models.module.jobs import JobRecord
from kiara.models.module.manifest import InputsManifest
from kiara.models.values.value import (
    SERIALIZE_TYPES,
    PersistedData,
    SerializedChunkIDs,
    SerializedData,
    Value,
)
from kiara.registries import FileSystemArchiveConfig
from kiara.registries.data.data_store import BaseDataStore, DataArchive
from kiara.registries.ids import ID_REGISTRY
from kiara.registries.jobs import JobArchive
from kiara.utils import log_message, orjson_dumps
from kiara.utils.hashfs import HashAddress, HashFS

if TYPE_CHECKING:
    pass

logger = structlog.getLogger()

VALUE_DETAILS_FILE_NAME = "value.json"


class EntityType(Enum):

    VALUE = "values"
    VALUE_DATA = "value_data"
    ENVIRONMENT = "environments"
    MANIFEST = "manifests"
    DESTINY_LINK = "destiny_links"


DEFAULT_HASHFS_DEPTH = 4
DEFAULT_HASHFS_WIDTH = 1
DEFAULT_HASH_FS_ALGORITHM = "sha256"


class FileSystemDataArchive(DataArchive, JobArchive):
    """Data store that loads data from the local filesystem."""

    _archive_type_name = "filesystem_data_archive"
    _config_cls = FileSystemArchiveConfig

    # @classmethod
    # def supported_item_types(cls) -> Iterable[str]:
    #
    #     return ["data", "job_record"]

    @classmethod
    def is_writeable(cls) -> bool:
        return False

    def __init__(self, archive_id: uuid.UUID, config: FileSystemArchiveConfig):

        DataArchive.__init__(self, archive_id=archive_id, config=config)
        self._base_path: Optional[Path] = None
        self._hashfs_path: Optional[Path] = None
        self._hashfs: Optional[HashFS] = None

    # def get_job_archive_id(self) -> uuid.UUID:
    #     return self._kiara.id

    @property
    def data_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self.config.archive_path).absolute()  # type: ignore
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

    def _delete_archive(self):
        shutil.rmtree(self.data_store_path)

    @property
    def hash_fs_path(self) -> Path:

        if self._hashfs_path is None:
            self._hashfs_path = self.data_store_path / "hash_fs"
        return self._hashfs_path

    @property
    def hashfs(self) -> HashFS:

        if self._hashfs is None:
            self._hashfs = HashFS(
                self.hash_fs_path.as_posix(),
                depth=DEFAULT_HASHFS_DEPTH,
                width=DEFAULT_HASHFS_WIDTH,
                algorithm=DEFAULT_HASH_FS_ALGORITHM,
            )
        return self._hashfs

    def get_path(
        self, entity_type: Optional[EntityType] = None, base_path: Optional[Path] = None
    ) -> Path:
        if base_path is None:
            if entity_type is None:
                result = self.data_store_path
            else:
                result = self.data_store_path / entity_type.value
        else:
            if entity_type is None:
                result = base_path
            else:
                result = base_path / entity_type.value

        result.mkdir(parents=True, exist_ok=True)
        return result

    def _retrieve_environment_details(
        self, env_type: str, env_hash: str
    ) -> Mapping[str, Any]:

        base_path = self.get_path(entity_type=EntityType.ENVIRONMENT)
        env_details_file = base_path / f"{env_type}_{env_hash}.json"

        if not env_details_file.exists():
            raise Exception(
                f"Can't load environment details, file does not exist: {env_details_file.as_posix()}"
            )

        environment = orjson.loads(env_details_file.read_text())
        return environment

    def find_matching_job_record(
        self, inputs_manifest: InputsManifest
    ) -> Optional[JobRecord]:

        return self._retrieve_job_record(
            manifest_hash=str(inputs_manifest.instance_cid),
            jobs_hash=inputs_manifest.job_hash,
        )

    def _retrieve_job_record(
        self, manifest_hash: str, jobs_hash: str
    ) -> Optional[JobRecord]:

        base_path = self.get_path(entity_type=EntityType.MANIFEST)
        manifest_folder = base_path / str(manifest_hash)

        if not manifest_folder.exists():
            return None

        manifest_file = manifest_folder / "manifest.json"

        if not manifest_file.exists():
            raise Exception(
                f"No 'manifests.json' file for manifest with hash: {manifest_hash}"
            )

        manifest_data = orjson.loads(manifest_file.read_text())

        job_folder = manifest_folder / jobs_hash

        if not job_folder.exists():
            return None

        inputs_file_name = job_folder / "inputs.json"
        if not inputs_file_name.exists():
            raise Exception(
                f"No 'inputs.json' file for manifest/inputs hash-combo: {manifest_hash} / {jobs_hash}"
            )

        inputs_data = {
            k: uuid.UUID(v)
            for k, v in orjson.loads(inputs_file_name.read_text()).items()
        }

        outputs = {}
        for output_file in job_folder.glob("output__*.json"):
            full_output_name = output_file.name[8:]
            start_value_id = full_output_name.find("__value_id__")
            output_name = full_output_name[0:start_value_id]
            value_id_str = full_output_name[start_value_id + 12 : -5]  # noqa

            value_id = uuid.UUID(value_id_str)
            outputs[output_name] = value_id

        job_id = ID_REGISTRY.generate(obj_type=JobRecord, desc="fake job id")
        job_record = JobRecord(
            job_id=job_id,
            module_type=manifest_data["module_type"],
            module_config=manifest_data["module_config"],
            inputs=inputs_data,
            outputs=outputs,
        )
        return job_record

    def _find_values_with_hash(
        self,
        value_hash: str,
        value_size: Optional[int] = None,
        data_type_name: Optional[str] = None,
    ) -> Set[uuid.UUID]:

        value_data_folder = self.get_path(entity_type=EntityType.VALUE_DATA)

        glob = f"*/{value_hash}/value_id__*.json"

        matches = list(value_data_folder.glob(glob))

        result = set()
        for match in matches:
            if not match.is_symlink():
                log_message(
                    f"Ignoring value_id file, not a symlink: {match.as_posix()}"
                )
                continue

            uuid_str = match.name[10:-5]
            value_id = uuid.UUID(uuid_str)
            result.add(value_id)

        return result

    def _find_destinies_for_value(
        self, value_id: uuid.UUID, alias_filter: Optional[str] = None
    ) -> Optional[Mapping[str, uuid.UUID]]:

        destiny_dir = self.get_path(entity_type=EntityType.DESTINY_LINK)
        destiny_value_dir = destiny_dir / str(value_id)

        if not destiny_value_dir.exists():
            return None

        destinies = {}
        for alias_link in destiny_value_dir.glob("*.json"):
            assert alias_link.is_symlink()

            alias = alias_link.name[0:-5]
            resolved = alias_link.resolve()

            value_id_str = resolved.parent.name
            value_id = uuid.UUID(value_id_str)
            destinies[alias] = value_id

        return destinies

    def _retrieve_all_value_ids(
        self, data_type_name: Optional[str] = None
    ) -> Iterable[uuid.UUID]:

        if data_type_name is not None:
            raise NotImplementedError()

        childs = self.get_path(entity_type=EntityType.VALUE).glob("*")
        folders = [uuid.UUID(x.name) for x in childs if x.is_dir()]
        return folders

    def has_value(self, value_id: uuid.UUID) -> bool:
        """Check whether the specific value_id is persisted in this data store.
        way to quickly determine whether a value id is valid for this data store.

        Arguments:
            value_id: the id of the value to check.
        Returns:
            whether this data store contains the value with the specified id
        """

        base_path = (
            self.get_path(entity_type=EntityType.VALUE)
            / str(value_id)
            / VALUE_DETAILS_FILE_NAME
        )
        return base_path.is_file()

    def _retrieve_value_details(self, value_id: uuid.UUID) -> Mapping[str, Any]:

        base_path = (
            self.get_path(entity_type=EntityType.VALUE)
            / str(value_id)
            / VALUE_DETAILS_FILE_NAME
        )
        if not base_path.is_file():
            raise Exception(
                f"Can't retrieve details for value with id '{value_id}': no value with that id stored."
            )

        value_data = orjson.loads(base_path.read_text())
        return value_data

    def _retrieve_serialized_value(self, value: Value) -> PersistedData:

        base_path = self.get_path(entity_type=EntityType.VALUE_DATA)
        data_dir = base_path / value.data_type_name / str(value.value_hash)

        serialized_value_file = data_dir / ".serialized_value.json"
        data = orjson.loads(serialized_value_file.read_text())

        return PersistedData(**data)

    def retrieve_chunk(
        self,
        chunk_id: str,
        as_file: Union[bool, str, None] = None,
        symlink_ok: bool = True,
    ) -> Union[bytes, str]:

        addr = self.hashfs.get(chunk_id)

        if as_file in (None, True):
            return addr.abspath
        elif as_file is False:
            return Path(addr.abspath).read_bytes()
        else:
            raise NotImplementedError()


class FilesystemDataStore(FileSystemDataArchive, BaseDataStore):
    """Data store that stores data as files on the local filesystem."""

    _archive_type_name = "filesystem_data_store"

    def _persist_environment_details(
        self, env_type: str, env_hash: str, env_data: Mapping[str, Any]
    ):

        base_path = self.get_path(entity_type=EntityType.ENVIRONMENT)
        env_details_file = base_path / f"{env_type}_{env_hash}.json"

        if not env_details_file.exists():
            env_details_file.write_text(orjson_dumps(env_data))

    def _persist_stored_value_info(self, value: Value, persisted_value: PersistedData):

        working_dir = self.get_path(entity_type=EntityType.VALUE_DATA)
        data_dir = working_dir / value.data_type_name / str(value.value_hash)
        sv_file = data_dir / ".serialized_value.json"
        data_dir.mkdir(exist_ok=True, parents=True)
        sv_file.write_text(persisted_value.json())

    def _persist_value_details(self, value: Value):

        value_dir = self.get_path(entity_type=EntityType.VALUE) / str(value.value_id)

        if value_dir.exists():
            raise Exception(
                f"Can't persist value '{value.value_id}', value directory already exists: {value_dir}"
            )
        else:
            value_dir.mkdir(parents=True, exist_ok=False)

        value_file = value_dir / VALUE_DETAILS_FILE_NAME
        value_data = value.dict()
        value_file.write_text(orjson_dumps(value_data, option=orjson.OPT_NON_STR_KEYS))

    def _persist_destiny_backlinks(self, value: Value):

        destiny_dir = self.get_path(entity_type=EntityType.DESTINY_LINK)

        for value_id, backlink in value.destiny_backlinks.items():

            destiny_value_dir = destiny_dir / str(value_id)
            destiny_value_dir.mkdir(parents=True, exist_ok=True)
            destiny_file = destiny_value_dir / f"{backlink}.json"
            assert not destiny_file.exists()

            value_dir = self.get_path(entity_type=EntityType.VALUE) / str(
                value.value_id
            )
            value_file = value_dir / VALUE_DETAILS_FILE_NAME
            assert value_file.exists()

            destiny_file.symlink_to(value_file)

    def _persist_value_data(self, value: Value) -> PersistedData:

        serialized_value: SerializedData = value.serialized_data

        chunk_id_map = {}
        for key in serialized_value.get_keys():

            data_model = serialized_value.get_serialized_data(key)

            if data_model.type == "chunk":  # type: ignore
                chunks: Iterable[Union[str, BytesIO]] = [BytesIO(data_model.chunk)]  # type: ignore
            elif data_model.type == "chunks":  # type: ignore
                chunks = (BytesIO(c) for c in data_model.chunks)  # type: ignore
            elif data_model.type == "file":  # type: ignore
                chunks = [data_model.file]  # type: ignore
            elif data_model.type == "files":  # type: ignore
                chunks = data_model.files  # type: ignore
            elif data_model.type == "inline-json":  # type: ignore
                chunks = [BytesIO(data_model.as_json())]  # type: ignore
            else:
                raise Exception(
                    f"Invalid serialized data type: {type(data_model)}. Available types: {', '.join(SERIALIZE_TYPES)}"
                )

            chunk_ids = []
            for item in zip(serialized_value.get_cids_for_key(key), chunks):
                cid = item[0]
                _chunk = item[1]
                addr: HashAddress = self.hashfs.put_with_precomputed_hash(
                    _chunk, str(cid)
                )
                chunk_ids.append(addr.id)

            scids = SerializedChunkIDs(
                chunk_id_list=chunk_ids,
                archive_id=self.archive_id,
                size=data_model.get_size(),
            )
            scids._data_registry = self.kiara_context.data_registry
            chunk_id_map[key] = scids

        pers_value = PersistedData(
            archive_id=self.archive_id,
            chunk_id_map=chunk_id_map,
            data_type=serialized_value.data_type,
            data_type_config=serialized_value.data_type_config,
            serialization_profile=serialized_value.serialization_profile,
            metadata=serialized_value.metadata,
        )

        return pers_value

    def _persist_value_pedigree(self, value: Value):

        manifest_hash = value.pedigree.instance_cid
        jobs_hash = value.pedigree.job_hash

        base_path = self.get_path(entity_type=EntityType.MANIFEST)
        manifest_folder = base_path / str(manifest_hash)
        manifest_folder.mkdir(parents=True, exist_ok=True)

        manifest_info_file = manifest_folder / "manifest.json"
        if not manifest_info_file.exists():
            manifest_info_file.write_text(value.pedigree.manifest_data_as_json())

        job_folder = manifest_folder / str(jobs_hash)

        job_folder.mkdir(parents=True, exist_ok=True)

        inputs_details_file_name = job_folder / "inputs.json"
        if not inputs_details_file_name.exists():
            inputs_details_file_name.write_text(orjson_dumps(value.pedigree.inputs))

        outputs_file_name = (
            job_folder
            / f"output__{value.pedigree_output_name}__value_id__{value.value_id}.json"
        )

        if outputs_file_name.exists():
            # if value.pedigree_output_name == "__void__":
            #     return
            # else:
            raise Exception(f"Can't write value '{value.value_id}': already exists.")
        else:
            outputs_file_name.touch()

        value_data_dir = (
            self.get_path(entity_type=EntityType.VALUE_DATA)
            / value.value_schema.type
            / str(value.value_hash)
        )
        target_file = value_data_dir / f"value_id__{value.value_id}.json"

        target_file.symlink_to(outputs_file_name)
