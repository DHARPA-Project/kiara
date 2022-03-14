# -*- coding: utf-8 -*-
import abc
import os
import shutil
import uuid
from enum import Enum
from pathlib import Path

import orjson
from rich.console import RenderableType
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from typing import TYPE_CHECKING, Optional, Iterable, Dict, Mapping, Any, Union, Tuple, List, Set
import structlog
from kiara.kiara.orm import ValueOrm, ValueTypeOrm, ManifestOrm
from kiara.models.module.jobs import JobConfig, JobRecord
from kiara.models.module.manifest import Manifest
from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.value import Value, ORPHAN, ValuePedigree, ValueDetails
from kiara.models.values.value_schema import ValueSchema
from kiara.modules.operations.included_core_operations.persistence import PersistValueOperationType
from kiara.utils import orjson_dumps, log_message
from kiara.value_types import ValueType
from kiara.value_types.included_core_types.persistence import LoadConfig, LoadConfigValue

if TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.modules import KiaraModule


logger = structlog.getLogger()

class DataStore(abc.ABC):

    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

    def store_value(self, value: Value) -> LoadConfig:

        if value.pedigree != ORPHAN:
            for value_id in value.pedigree.inputs.values():
                if not self.contains_value(value_id=value_id):
                    other = self._kiara.data_registry.get_value(value=value_id)
                    assert other and other.value_id == value_id
                    self.store_value(other)

        logger.debug("value.store", value_type=value.value_schema.type, value_id=value.id, value_hash=value.value_hash)
        load_config = self.persist_value(value)
        return load_config

    @property
    def value_ids(self) -> Iterable[uuid.UUID]:
        return self.retrieve_all_value_ids()

    def retrieve_job(self, job: JobConfig) -> Optional[JobRecord]:

        return self.retrieve_job_record(manifest_hash=job.manifest_hash, inputs_hash=job.inputs_hash)

    # @abc.abstractmethod
    # def persist_environment(self, environment: RuntimeEnvironment):
    #     pass

    # @abc.abstractmethod
    # def persist_job_record(self, job_record: JobRecord):
    #     pass

    @abc.abstractmethod
    def retrieve_job_record(self, manifest_hash: int, inputs_hash: int) -> Optional[JobRecord]:
        pass

    @abc.abstractmethod
    def persist_value(self, value: Value) -> LoadConfig:
        pass

    @abc.abstractmethod
    def retrieve_environment_details(self, env_type: str, env_hash: int) -> Mapping[str, Any]:
        pass

    @abc.abstractmethod
    def retrieve_value(self, value_id: uuid.UUID) -> Value:
        pass

    @abc.abstractmethod
    def find_values_for_hash(self, value_hash: int, value_type: Optional[str]=None) -> Optional[Set[Value]]:
        pass

    @abc.abstractmethod
    def retrieve_all_value_ids(self) -> Iterable[uuid.UUID]:
        pass

    def contains_value(self, value_id: uuid.UUID) -> bool:
        """Check whether the specific value_id is persisted in this data store.

        DataStore implementations should override this method for better performance.
        """

        return value_id in self.retrieve_all_value_ids()

    def create_renderable(self, **config: Any) -> RenderableType:
        """Create a renderable for this module configuration."""

        from kiara.utils.output import RenderConfig, create_renderable_from_values

        table = create_renderable_from_values(values={str(i): self.retrieve_value(i) for i in self.value_ids}, config=config)
        return table

class EntityType(Enum):

    VALUE = "values"
    VALUE_DATA = "value_data"
    ENVIRONMENT = "environments"
    MANIFEST = "manifests"

class CachedDataStore(DataStore):

    def __init__(self, kiara: "Kiara"):
        super().__init__(kiara=kiara)

        self._env_cache: Dict[str, Dict[int, Mapping[str, Any]]] = {}
        self._job_cache: Dict[int, Dict[int, JobRecord]] = {}
        self._value_cache: Dict[uuid.UUID, Optional[Value]] = {}
        self._value_hash_index: Dict[int, set(uuid.UUID)] = {}
        # self._value_hash_by_type_index: Dict[str, Dict[int, uuid.UUID]] = {}
        self._load_configs: Dict[uuid.UUID, LoadConfig] = {}
        self._all_value_ids_loaded: bool = False

    @abc.abstractmethod
    def _persist_environment(self, env_type: str, env_hash: int, env_data: Mapping[str, Any]):
        pass

    @abc.abstractmethod
    def _persist_manifest(self, manifest: Manifest):
        pass

    @abc.abstractmethod
    def _persist_value(self, value: Value) -> Tuple[Mapping[str, Any], LoadConfig]:
        pass

    @abc.abstractmethod
    def _link_value_output(self, pedigree: ValuePedigree, value: Value):
         pass

    @abc.abstractmethod
    def _retrieve_environment(self, env_type: str, env_hash: int):
        pass

    @abc.abstractmethod
    def _retrieve_job_record(self, manifest_hash: int, inputs_hash: int) -> Mapping[str, Any]:
        pass

    @abc.abstractmethod
    def _retrieve_all_value_ids(self) -> Iterable[uuid.UUID]:
        pass

    @abc.abstractmethod
    def _retrieve_value_details(self, value_id: uuid.UUID) -> Mapping[str, Any]:
        pass

    @abc.abstractmethod
    def _retrieve_load_config(self, value: Value) -> LoadConfig:
        pass

    @abc.abstractmethod
    def _find_values_for_hash(self, value_hash: int, value_type: Optional[str]=None) -> Optional[Set[uuid.UUID]]:
        pass


    def persist_environment(self, environment: RuntimeEnvironment):

        env_type = environment.get_environment_type_name()
        env_hash = environment.model_data_hash

        env = self._env_cache.get(env_type, {}).get(env_hash, None)
        if env is not None:
            return

        env_data = environment.as_dict_with_schema()
        self._persist_environment(env_type=env_type, env_hash=env_hash, env_data=env_data)
        self._env_cache.setdefault(env_type, {})[env_hash] = env_data

    # def persist_job_record(self, job_record: JobRecord):
    #
    #     if job_record.job_hash in self._job_cache.keys():
    #         raise Exception(f"Job with hash '{job_record.job_hash}' already persisted.")
    #
    #     self._persist_job_record(job_record=job_record)
    #     self._job_cache[job_record.job_hash] = job_record

    def retrieve_job_record(self, manifest_hash: int, inputs_hash) -> Optional[JobRecord]:

        cached = self._job_cache.get(manifest_hash, {}).get(inputs_hash, None)
        if cached is not None:
            return cached

        job_record = self._retrieve_job_record(manifest_hash=manifest_hash, inputs_hash=inputs_hash)
        if job_record is None:
            return None

        self._job_cache.setdefault(manifest_hash, {})[inputs_hash] = job_record
        return job_record

    def persist_value(self, value: Value) -> LoadConfig:
        if value.value_id in self._value_cache.keys():
            raise Exception(f"Value with id '{value.value_id}' already stored.")

        # first, persist environment information
        for env_hash in value.pedigree.environments.values():
            env = self._kiara.runtime_env_mgmt.get_environment_for_hash(env_hash)
            self.persist_environment(env)

        # then, make sure the manifest is persisted
        if value.pedigree != ORPHAN:
            self._persist_manifest(value.pedigree)

        # now, save the value data and metadata
        (value_data, load_config) = self._persist_value(value)

        # now link the output values to the manifest
        self._link_value_output(pedigree=value.pedigree, value=value)

        self._value_cache[value.value_id] = value
        self._load_configs[value.value_id] = load_config
        return load_config

    def retrieve_all_value_ids(self) -> Iterable[uuid.UUID]:

        if self._all_value_ids_loaded:
            return self._value_cache.items()

        all_ids = {k: None for k in self._retrieve_all_value_ids() if k not in self._value_cache.keys()}
        self._value_cache.update(all_ids)
        self._all_value_ids_loaded = True
        return self._value_cache.keys()

    def contains_value(self, value_id: uuid.UUID) -> bool:
        """Check whether the specific value_id is persisted in this data store.
        """

        try:
            self.retrieve_value(value_id=value_id)
            return True
        except Exception:
            return False

    def retrieve_value(self, value_id: uuid.UUID) -> Value:

        cached = self._value_cache.get(value_id, None)
        if cached is not None:
            return cached

        value_data = self._retrieve_value_details(value_id=value_id)

        value_schema = ValueSchema(**value_data["value_schema"])
        value_type = self._kiara.get_value_type(
                value_type=value_schema.type, value_type_config=value_schema.type_config
            )

        pedigree = ValuePedigree(**value_data["pedigree"])

        value = LoadConfigValue(
            value_id = value_data["value_id"],
            kiara_id = self._kiara.id,
            value_schema = value_schema,
            value_status = value_data["value_status"],
            value_size = value_data["value_size"],
            value_hash = value_data["value_hash"],
            pedigree = pedigree,
            pedigree_output_name=value_data["pedigree_output_name"]
        )

        # value = value_type.reassemble_value(value_id=value_data["value_id"], load_config=None, schema=value_schema, status=value_data["value_status"], value_hash=value_data["value_hash"], value_size=value_data["value_size"], pedigree=pedigree, kiara_id=self._kiara.id, pedigree_output_name=value_data["pedigree_output_name"])

        load_config = self._retrieve_load_config(value=value)
        value.init_data(load_config=load_config, jobs_mgmt=self._kiara.jobs_mgmt)

        self._value_cache[value_id] = value
        return self._value_cache[value_id]

    def find_values_for_hash(self, value_hash: int, value_type: Optional[str]=None) -> Set[Value]:

        if value_type is not None:
            raise NotImplementedError()

        if value_hash in self._value_hash_index.keys():
            value_ids = self._value_hash_index[value_hash]
        else:
            value_ids = self._find_values_for_hash(value_hash=value_hash, value_type=value_type)
            self._value_hash_index[value_hash] = value_ids

        return set((self.retrieve_value(value_id) for value_id in value_ids))

    def retrieve_environment_details(self, env_type: str, env_hash: int) -> Mapping[str, Any]:

        env = self._env_cache.get(env_type, {}).get(env_hash, None)
        if env is not None:
            return env

        env = self._retrieve_environment(env_type=env_type, env_hash=env_hash)
        self._env_cache.setdefault(env_type, {})[env_hash] = env
        return env



VALUE_DETAILS_FILE_NAME = "value.json"

class FilesystemDataStore(CachedDataStore):

    def __init__(self, kiara: "Kiara"):

        super().__init__(kiara=kiara)
        self._base_path: Optional[Path] = None

    @property
    def data_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self._kiara.context_config.data_directory) / "store"
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

    def get_path(self, entity_type: Optional[EntityType] = None, base_path: Optional[Path]=None) -> Path:
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

    def _persist_environment(self, env_type: str, env_hash: int, env_data: Mapping[str, Any]):

        base_path = self.get_path(entity_type=EntityType.ENVIRONMENT)
        env_details_file = base_path / f"{env_type}_{env_hash}.json"

        if not env_details_file.exists():
            env_details_file.write_text(orjson_dumps(env_data))

    def _retrieve_environment(self, env_type: str, env_hash: int) -> Mapping[str, Any]:

        base_path = self.get_path(entity_type=EntityType.ENVIRONMENT)
        env_details_file = base_path / f"{env_type}_{env_hash}.json"

        if not env_details_file.exists():
            raise Exception(f"Can't load environment details, file does not exist: {env_details_file.as_posix()}")

        environment = orjson.loads(env_details_file.read_text())
        return environment

    def _persist_value(self, value: Value) -> Tuple[Mapping[str, Any], LoadConfig]:

        value_dir = self.get_path(entity_type=EntityType.VALUE) / str(value.value_id)

        if value_dir.exists():
            raise Exception(f"Can't persist value '{value.value_id}', value directory already exists: {value_dir}")
        else:
            value_dir.mkdir(parents=True, exist_ok=False)
        load_config = self._persist_value_data(value=value)
        value_file = value_dir / VALUE_DETAILS_FILE_NAME
        value_data = value.dict()
        value_file.write_text(orjson_dumps(value_data))

        return (value_data, load_config)

    def _persist_value_data(self, value: Value) -> LoadConfig:

        persist_op_type = self._kiara.operations_mgmt.operation_types.get("persist_value", None)
        if persist_op_type is None:
            raise Exception("Can't persist value, 'persist_value' operation type not available.")

        op_type: PersistValueOperationType = self._kiara.operations_mgmt.get_operation_type("persist_value")  # type: ignore
        op = op_type.get_operation_for_value_type(value.value_schema.type)

        working_dir = self.get_path(entity_type=EntityType.VALUE_DATA)
        data_dir = working_dir / value.value_type_name / str(value.value_hash)

        base_name = "value"
        result = op.run(kiara=self._kiara, inputs={"value": value, "target": data_dir.as_posix(), "base_name": base_name})

        load_config: LoadConfig = result.get_value_data("load_config")

        load_config_file = data_dir / ".load_config.json"
        load_config_file.write_text(load_config.json())

        return load_config

    def _persist_manifest(self, manifest: Manifest):

        base_path = self.get_path(entity_type=EntityType.MANIFEST)
        manifest_folder = base_path / str(manifest.manifest_hash)

        if manifest_folder.exists():
            return
        else:
            manifest_folder.mkdir(parents=True, exist_ok=False)

        manifest_info_file = manifest_folder / "manifest.json"
        manifest_info_file.write_text(orjson_dumps(manifest.manifest_data))

    def _retrieve_job_record(self, manifest_hash: int, inputs_hash: int) -> Optional[Mapping[str, Any]]:

        base_path = self.get_path(entity_type=EntityType.MANIFEST)
        manifest_folder = base_path / str(manifest_hash)

        if not manifest_folder.exists():
            return None

        manifest_file = manifest_folder / "manifest.json"

        if not manifest_file.exists():
            raise Exception(
                f"No 'manifests.json' file for manifest with hash: {manifest_hash}")

        manifest_data = orjson.loads(manifest_file.read_text())

        inputs_folder = manifest_folder / str(inputs_hash)

        if not inputs_folder.exists():
            return None

        inputs_file_name = inputs_folder / "inputs.json"
        if not inputs_file_name.exists():
            raise Exception(
                f"No 'inputs.json' file for manifest/inputs hash-combo: {manifest_hash} / {inputs_hash}")

        inputs_data = {k: uuid.UUID(v) for k, v in orjson.loads(inputs_file_name.read_text()).items()}

        outputs = {}
        for output_file in inputs_folder.glob("output_*.json"):
            full_output_name = output_file.name[8:]
            start_value_id = full_output_name.find("__value_id__")
            output_name = full_output_name[0:start_value_id]
            value_id_str = full_output_name[start_value_id+12:-5]

            value_id = uuid.UUID(value_id_str)
            outputs[output_name] = value_id

        job_record = JobRecord(module_type=manifest_data["module_type"], module_config=manifest_data["module_config"], inputs=inputs_data, outputs=outputs)
        return job_record

    def _link_value_output(self, pedigree: ValuePedigree, value: Value):

        base_path = self.get_path(entity_type=EntityType.MANIFEST)
        if pedigree == ORPHAN:
            manifest_folder = base_path / "0"
            inputs_folder = manifest_folder / "0"
            inputs_folder.mkdir(parents=True, exist_ok=True)

        else:
            manifest_folder = base_path / str(pedigree.manifest_hash)
            inputs_folder = manifest_folder / str(pedigree.inputs_hash)

            inputs_folder.mkdir(parents=True, exist_ok=True)

            inputs_details_file_name = inputs_folder / "inputs.json"
            if not inputs_details_file_name.exists():
                inputs_details_file_name.write_text(orjson_dumps(pedigree.inputs))

        outputs_file_name = inputs_folder / f"output__{value.pedigree_output_name}__value_id__{value.value_id}.json"

        if outputs_file_name.exists():
            # if value.pedigree_output_name == "__void__":
            #     return
            # else:
                raise Exception(f"Can't write value '{value.value_id}': already exists.")
        else:
            outputs_file_name.touch()

        value_data_dir = self.get_path(entity_type=EntityType.VALUE_DATA) / value.value_schema.type / str(value.value_hash)
        target_file = value_data_dir / f"value_id__{value.value_id}.json"

        target_file.symlink_to(outputs_file_name)

    def _find_values_for_hash(self, value_hash: int, value_type: Optional[str]=None) -> Set[uuid.UUID]:

        value_data_folder = self.get_path(entity_type=EntityType.VALUE_DATA)

        glob = f"*/{value_hash}/value_id__*.json"

        matches = list(value_data_folder.glob(glob))

        result = set()
        for match in matches:
            if not match.is_symlink():
                log_message(f"Ignoring value_id file, not a symlink: {match.as_posix()}")
                continue

            uuid_str = match.name[10:-5]
            value_id = uuid.UUID(uuid_str)
            result.add(value_id)

        return result

    def _retrieve_all_value_ids(self) -> Iterable[uuid.UUID]:

        childs = self.get_path(entity_type=EntityType.VALUE).glob("*")
        folders = [uuid.UUID(x.name) for x in childs if x.is_dir()]
        return folders

    def _retrieve_value_details(self, value_id: uuid.UUID) -> Mapping[str, Any]:

        base_path = self.get_path(entity_type=EntityType.VALUE) / str(value_id) / VALUE_DETAILS_FILE_NAME
        value_data = orjson.loads(base_path.read_text())
        return value_data

    def _retrieve_load_config(self, value: Value) -> LoadConfig:

        base_path = self.get_path(entity_type=EntityType.VALUE_DATA)
        data_dir = base_path / value.value_type_name / str(value.value_hash)

        load_config_file = data_dir / ".load_config.json"
        data = orjson.loads(load_config_file.read_text())

        return LoadConfig(**data)


# class PersistenceMgmt(object):
#     def __init__(self, kiara: "Kiara"):
#
#         self._kiara: Kiara = kiara
#         self._engine: Engine = self._kiara._engine
#
#     # def _serialize_value(self, value: Value) -> SerializedValueModel:
#     #
#     #     serialize_op_type = self._kiara.operations_mgmt.operation_types.get("serialize", None)
#     #     if serialize_op_type is None:
#     #         raise Exception("Can't serialize value, 'serialize' operation type not available.")
#     #
#     #     s_value = self._kiara.operations_mgmt.apply_operation(operation_type="serialize", value=value)
#     #     serialized: SerializedValueModel = s_value.serialized_value.data
#     #
#     #     return serialized
#
#     def _persist_value_data(self, value: Value) -> LoadConfig:
#
#         persist_op_type = self._kiara.operations_mgmt.operation_types.get("persist_value", None)
#         if persist_op_type is None:
#             raise Exception("Can't persist value, 'persist_value' operation type not available.")
#
#         op_type: PersistValueOperationType = self._kiara.operations_mgmt.get_operation_type("persist_value")  # type: ignore
#         op = op_type.get_operation_for_value_type(value.value_schema.type)
#         base_path = os.path.join(self._kiara.context_config.data_directory, str(value.value_id))
#         base_name = "value"
#
#         result = op.run(kiara=self._kiara, inputs={"value": value, "target": base_path, "base_name": base_name})
#
#         return result.get_value_data("load_config")
#
#     def _persist_value_in_session(self, value: Value, session: Session) -> ValueOrm:
#
#         if value.data is None:
#             raise NotImplementedError()
#
#         persisted = self._persist_value_data(value)
#         return None
#
#         value_type: ValueType = self._kiara.get_value_type(
#             value_type=value.value_schema.type, value_type_config=value.value_schema.type_config
#         )
#
#         size = value.value_size
#         hash = value.value_hash
#
#         value_type_orm = (
#             session.query(ValueTypeOrm)
#             .filter_by(
#                 type_config_hash=value_type.value_type_hash,
#                 type_name=value_type.value_type_name,
#             )
#             .first()
#         )
#         if value_type_orm is None:
#             value_type_orm = ValueTypeOrm(
#                 type_config_hash=value_type.value_type_hash,
#                 type_name=value_type.value_type_name,
#                 type_config=value_type.type_config.dict(),
#             )
#             session.add(value_type_orm)
#             session.commit()
#
#         value_orm = (
#             session.query(ValueOrm)
#             .filter_by(
#                 value_hash=hash,
#                 value_size=size,
#                 value_type_name=value_type.value_type_name,
#             )
#             .first()
#         )
#         if value_orm is None:
#             value_id = uuid.uuid4()
#             value_orm = ValueOrm(
#                 global_id=value_id,
#                 value_type_name=value_type.value_type_name,
#                 value_size=size,
#                 value_hash=hash,
#                 value_type_id=value_type_orm.id,
#             )
#             session.add(value_orm)
#             session.commit()
#
#         return value_orm
#
#     def persist_value(self, value: Value):
#
#         with Session(bind=self._engine, future=True) as session:
#             value_orm = self._persist_value_in_session(value=value, session=session)
#
#         return value_orm
#
#     def persist_values(self, **values: Value):
#
#         orm_values = {}
#         with Session(bind=self._engine, future=True) as session:
#
#             for field, value in values.items():
#
#                 value_orm = self._persist_value_in_session(value=value, session=session)
#                 orm_values[field] = value_orm
#
#         return orm_values
#
#     def persist_module(self, module: "KiaraModule") -> ManifestOrm:
#
#         with Session(bind=self._engine, future=True) as session:
#             m = (
#                 session.query(ManifestOrm)
#                 .filter_by(module_config_hash=module.module_instance_hash)
#                 .first()
#             )
#
#             if m is None:
#                 mc = ManifestOrm(
#                     module_type=module.module_type_id,
#                     module_config_hash=module.module_instance_hash,
#                     module_config=module.config.dict(),
#                     is_idempotent=module.is_idempotent,
#                 )
#                 session.add(mc)
#                 session.commit()
#
#         return m
