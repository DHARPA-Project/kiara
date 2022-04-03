# -*- coding: utf-8 -*-
import orjson
import structlog
import uuid
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Set

from kiara.kiara.data_store import DataArchive, DataStore
from kiara.kiara.job_registry import JobArchive
from kiara.models.module.destiniy import Destiny
from kiara.models.module.jobs import JobRecord
from kiara.models.module.manifest import InputsManifest, LoadConfig
from kiara.models.values.value import Value
from kiara.modules.operations.included_core_operations.persistence import (
    PersistValueOperationType,
)
from kiara.utils import log_message, orjson_dumps

if TYPE_CHECKING:
    from kiara.kiara import Kiara

logger = structlog.getLogger()

VALUE_DETAILS_FILE_NAME = "value.json"


class EntityType(Enum):

    VALUE = "values"
    VALUE_DATA = "value_data"
    ENVIRONMENT = "environments"
    MANIFEST = "manifests"
    DESTINY = "destinies"


class FileSystemArchive(DataArchive, JobArchive):
    def __init__(self, kiara: "Kiara"):

        DataArchive.__init__(self, kiara=kiara)
        self._base_path: Optional[Path] = None

    def get_job_archive_id(self) -> uuid.UUID:
        return self._kiara.id

    @property
    def data_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self._kiara.context_config.data_directory) / "data_store"
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

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
        self, env_type: str, env_hash: int
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
            manifest_hash=inputs_manifest.manifest_hash,
            jobs_hash=inputs_manifest.job_hash,
        )

    def _retrieve_job_record(
        self, manifest_hash: int, jobs_hash: int
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

        job_folder = manifest_folder / str(jobs_hash)

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
            value_id_str = full_output_name[start_value_id + 12 : -5]

            value_id = uuid.UUID(value_id_str)
            outputs[output_name] = value_id

        job_record = JobRecord(
            module_type=manifest_data["module_type"],
            module_config=manifest_data["module_config"],
            inputs=inputs_data,
            outputs=outputs,
        )
        return job_record

    def _find_values_with_hash(
        self,
        value_hash: int,
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

    def _retrieve_load_config(self, value: Value) -> LoadConfig:

        base_path = self.get_path(entity_type=EntityType.VALUE_DATA)
        data_dir = base_path / value.data_type_name / str(value.value_hash)

        load_config_file = data_dir / ".load_config.json"
        data = orjson.loads(load_config_file.read_text())

        return LoadConfig(**data)


class FilesystemDataStore(FileSystemArchive, DataStore):
    def _persist_environment_details(
        self, env_type: str, env_hash: int, env_data: Mapping[str, Any]
    ):

        base_path = self.get_path(entity_type=EntityType.ENVIRONMENT)
        env_details_file = base_path / f"{env_type}_{env_hash}.json"

        if not env_details_file.exists():
            env_details_file.write_text(orjson_dumps(env_data))

    def _persist_value(self, value: Value) -> LoadConfig:

        value_dir = self.get_path(entity_type=EntityType.VALUE) / str(value.value_id)

        if value_dir.exists():
            raise Exception(
                f"Can't persist value '{value.value_id}', value directory already exists: {value_dir}"
            )
        else:
            value_dir.mkdir(parents=True, exist_ok=False)

        load_config = self._persist_value_data(value=value)
        value_file = value_dir / VALUE_DETAILS_FILE_NAME
        value_data = value.dict()
        value_file.write_text(orjson_dumps(value_data))

        return load_config

    def _persist_value_data(self, value: Value) -> LoadConfig:

        persist_op_type = self._kiara.operation_registry.operation_types.get(
            "persist_value", None
        )
        if persist_op_type is None:
            raise Exception(
                "Can't persist value, 'persist_value' operation type not available."
            )

        op_type: PersistValueOperationType = self._kiara.operation_registry.get_operation_type("persist_value")  # type: ignore
        op = op_type.get_operation_for_data_type(value.value_schema.type)

        working_dir = self.get_path(entity_type=EntityType.VALUE_DATA)
        data_dir = working_dir / value.data_type_name / str(value.value_hash)

        base_name = "value"
        result = op.run(
            kiara=self._kiara,
            inputs={
                "value": value,
                "target": data_dir.as_posix(),
                "base_name": base_name,
            },
        )

        load_config: LoadConfig = result.get_value_data("load_config")
        if not load_config:
            raise Exception(
                f"Can't write load config, no load config returned by module '{op.module_type}' when persisting value."
            )

        if not isinstance(load_config, LoadConfig):
            raise Exception(
                f"Can't write load config, invalid result type '{type(load_config)}' from module '{op.module_type}' when persisting value."
            )

        load_config_file = data_dir / ".load_config.json"
        load_config_file.write_text(load_config.json())

        return load_config

    # def _persist_manifest(self, manifest: Manifest):
    #
    #     base_path = self.get_path(entity_type=EntityType.MANIFEST)
    #     manifest_folder = base_path / str(manifest.manifest_hash)
    #
    #     if manifest_folder.exists():
    #         return
    #     else:
    #         manifest_folder.mkdir(parents=True, exist_ok=False)
    #
    #     manifest_info_file = manifest_folder / "manifest.json"
    #     manifest_info_file.write_text(orjson_dumps(manifest.manifest_data))

    def _persist_value_pedigree(self, value: Value):

        manifest_hash = value.pedigree.manifest_hash
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

    def _persist_destinies(
        self, value: Value, category: str, key: str, destinies: Set[Destiny]
    ):

        base_path = self.get_path(EntityType.DESTINY)

        for destiny in destinies:
            path = (
                base_path
                / str(value.value_id)
                / category
                / key
                / f"destiny__{destiny.destiny_id}.json"
            )
            if path.exists():
                raise Exception(
                    f"Can't persist destiny '{destiny.destiny_id}': already persisted."
                )

            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(orjson_dumps(destiny.dict()))
