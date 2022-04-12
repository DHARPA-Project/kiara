# -*- coding: utf-8 -*-
import abc
import structlog
import uuid
from rich.console import RenderableType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from kiara.models.module.persistence import (
    ByteProvisioningStrategy,
    BytesAliasStructure,
    BytesStructure,
    LoadConfig,
)
from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.value import Value, ValuePedigree
from kiara.models.values.value_schema import ValueSchema
from kiara.registries import ARCHIVE_CONFIG_CLS, BaseArchive

if TYPE_CHECKING:
    pass

logger = structlog.getLogger()


class DataArchive(BaseArchive):
    @classmethod
    def supported_item_types(cls) -> Iterable[str]:

        return ["data", "job_record"]

    def __init__(self, archive_id: uuid.UUID, config: ARCHIVE_CONFIG_CLS):

        super().__init__(archive_id=archive_id, config=config)

        self._env_cache: Dict[str, Dict[int, Mapping[str, Any]]] = {}
        self._value_cache: Dict[uuid.UUID, Value] = {}
        self._load_config_cache: Dict[uuid.UUID, LoadConfig] = {}
        self._value_hash_index: Dict[int, Set[uuid.UUID]] = {}

    def retrieve_load_config(self, value: Union[uuid.UUID, Value]) -> LoadConfig:

        if isinstance(value, Value):
            value_id = value.value_id
        else:
            value_id = value
            value = None

        if value_id in self._load_config_cache.keys():
            return self._load_config_cache[value_id]

        if value is None:
            value = self.retrieve_value(value_id)

        load_config = self._retrieve_load_config(value=value)
        self._load_config_cache[value.value_id] = load_config
        return load_config

    @abc.abstractmethod
    def _retrieve_load_config(self, value: Value) -> LoadConfig:
        pass

    def retrieve_value(self, value_id: uuid.UUID) -> Value:

        cached = self._value_cache.get(value_id, None)
        if cached is not None:
            return cached

        value_data = self._retrieve_value_details(value_id=value_id)

        value_schema = ValueSchema(**value_data["value_schema"])
        # data_type = self._kiara.get_value_type(
        #         data_type=value_schema.type, data_type_config=value_schema.type_config
        #     )

        pedigree = ValuePedigree(**value_data["pedigree"])

        value = Value(
            value_id=value_data["value_id"],
            kiara_id=self.kiara_context.id,
            value_schema=value_schema,
            value_status=value_data["value_status"],
            value_size=value_data["value_size"],
            value_hash=value_data["value_hash"],
            pedigree=pedigree,
            pedigree_output_name=value_data["pedigree_output_name"],
            data_type_class=value_data["data_type_class"],
            property_refs=value_data["property_refs"],
            destiny_details=value_data["destiny_details"],
        )

        self._value_cache[value_id] = value
        return self._value_cache[value_id]

    @abc.abstractmethod
    def _retrieve_value_details(self, value_id: uuid.UUID) -> Mapping[str, Any]:
        pass

    @property
    def value_ids(self) -> Iterable[uuid.UUID]:
        return self._retrieve_all_value_ids()

    @abc.abstractmethod
    def _retrieve_all_value_ids(
        self, data_type_name: Optional[str] = None
    ) -> Iterable[uuid.UUID]:
        pass

    def has_value(self, value_id: uuid.UUID) -> bool:
        """Check whether the specific value_id is persisted in this data store.

        Implementing classes are encouraged to override this method, and choose a suitable, implementation specific
        way to quickly determine whether a value id is valid for this data store.

        Arguments:
            value_id: the id of the value to check.
        Returns:
            whether this data store contains the value with the specified id
        """

        return value_id in self._retrieve_all_value_ids()

    def retrieve_environment_details(
        self, env_type: str, env_hash: int
    ) -> Mapping[str, Any]:
        """Retrieve the environment details with the specified type and hash.

        The environment is stored by the data store as a dictionary, including it's schema, not as the actual Python model.
        This is to make sure it can still be loaded later on, in case the Python model has changed in later versions.
        """

        cached = self._env_cache.get(env_type, {}).get(env_hash, None)
        if cached is not None:
            return cached

        env = self._retrieve_environment_details(env_type=env_type, env_hash=env_hash)
        self._env_cache.setdefault(env_type, {})[env_hash] = env
        return env

    @abc.abstractmethod
    def _retrieve_environment_details(
        self, env_type: str, env_hash: int
    ) -> Mapping[str, Any]:
        pass

    def find_values_with_hash(
        self,
        value_hash: int,
        value_size: Optional[int] = None,
        data_type_name: Optional[str] = None,
    ) -> Set[uuid.UUID]:

        if data_type_name is not None:
            raise NotImplementedError()

        if value_size is not None:
            raise NotImplementedError()

        if value_hash in self._value_hash_index.keys():
            value_ids = self._value_hash_index[value_hash]
        else:
            value_ids = self._find_values_with_hash(
                value_hash=value_hash, data_type_name=data_type_name
            )
            if value_ids is None:
                value_ids = set()
            self._value_hash_index[value_hash] = value_ids

        return value_ids

    @abc.abstractmethod
    def _find_values_with_hash(
        self,
        value_hash: int,
        value_size: Optional[int] = None,
        data_type_name: Optional[str] = None,
    ) -> Optional[Set[Value]]:
        pass

    @abc.abstractmethod
    def retrieve_bytes(self, chunk_id: str, as_bytes: bool = True) -> Union[bytes, str]:
        pass

    # def retrieve_job_record(self, inputs_manifest: InputsManifest) -> Optional[JobRecord]:
    #     return self._retrieve_job_record(
    #         manifest_hash=inputs_manifest.manifest_hash, jobs_hash=inputs_manifest.jobs_hash
    #     )
    #
    # @abc.abstractmethod
    # def _retrieve_job_record(
    #     self, manifest_hash: int, jobs_hash: int
    # ) -> Optional[JobRecord]:
    #     pass


class DataStore(DataArchive):
    @classmethod
    def is_writeable(cls) -> bool:
        return True

    @abc.abstractmethod
    def store_value(self, value: Value) -> LoadConfig:
        """ "Store the value, its data and metadata into the store.

        Arguments:
            value: the value to persist

        Returns:
            the load config that is needed to retrieve the value data later
        """


class BaseDataStore(DataStore):
    @abc.abstractmethod
    def _persist_bytes(self, bytes_structure: BytesStructure) -> BytesAliasStructure:
        pass

    @abc.abstractmethod
    def _persist_load_config(self, value: Value, load_config: LoadConfig):
        pass

    @abc.abstractmethod
    def _persist_value_details(self, value: Value):
        pass

    @abc.abstractmethod
    def _persist_value_data(
        self, value: Value
    ) -> Tuple[LoadConfig, Optional[BytesStructure]]:
        pass

    @abc.abstractmethod
    def _persist_value_pedigree(self, value: Value):
        """Create an internal link from a value to its pedigree (and pedigree details).

        This is so that the 'retrieve_job_record' can be used to prevent running the same job again, and the link of value
        to the job that produced it is preserved.
        """

    @abc.abstractmethod
    def _persist_environment_details(
        self, env_type: str, env_hash: int, env_data: Mapping[str, Any]
    ):
        pass

    def store_value(self, value: Value) -> LoadConfig:

        logger.debug(
            "store.value",
            data_type=value.value_schema.type,
            value_id=value.value_id,
            value_hash=value.value_hash,
        )

        # first, persist environment information
        for env_type, env_hash in value.pedigree.environments.items():
            cached = self._env_cache.get(env_type, {}).get(env_hash, None)
            if cached is not None:
                continue

            env = self.kiara_context.environment_registry.get_environment_for_hash(
                env_hash
            )
            self.persist_environment(env)

        # save the value data and metadata
        load_config = self._persist_value(value)
        self._load_config_cache[value.value_id] = load_config
        self._value_cache[value.value_id] = value
        self._value_hash_index.setdefault(value.value_hash, set()).add(value.value_id)

        # now link the output values to the manifest
        # then, make sure the manifest is persisted
        self._persist_value_pedigree(value=value)

        return load_config

    def _persist_value(self, value: Value) -> LoadConfig:

        # TODO: check if value id is already persisted?
        load_config, bytes_structure = self._persist_value_data(value=value)

        if not load_config:
            raise Exception(
                f"Can't write load config, no load config returned when persisting value."
            )
        if not isinstance(load_config, LoadConfig):
            raise Exception(
                f"Can't write load config, invalid result type '{type(load_config)}' when persisting value."
            )

        if (
            load_config.provisioning_strategy != ByteProvisioningStrategy.INLINE
            and not bytes_structure
        ):
            raise Exception(
                f"Can't write load config, no bytes structure returned when persisting value."
            )

        if bytes_structure and not isinstance(bytes_structure, BytesStructure):
            raise Exception(
                f"Can't write load config, invalid result bytes structure type '{type(bytes_structure)}' when persisting value."
            )

        if bytes_structure:
            alias_structure = self._persist_bytes(bytes_structure=bytes_structure)
            load_config.bytes_map = alias_structure
        self._persist_load_config(value=value, load_config=load_config)
        self._persist_value_details(value=value)

        return load_config

    def persist_environment(self, environment: RuntimeEnvironment):
        """Persist the specified environment.

        The environment is stored as a dictionary, including it's schema, not as the actual Python model.
        This is to make sure it can still be loaded later on, in case the Python model has changed in later versions.
        """

        env_type = environment.get_environment_type_name()
        env_hash = environment.model_data_hash

        env = self._env_cache.get(env_type, {}).get(env_hash, None)
        if env is not None:
            return

        env_data = environment.as_dict_with_schema()
        self._persist_environment_details(
            env_type=env_type, env_hash=env_hash, env_data=env_data
        )
        self._env_cache.setdefault(env_type, {})[env_hash] = env_data

    def create_renderable(self, **config: Any) -> RenderableType:
        """Create a renderable for this module configuration."""

        from kiara.utils.output import create_renderable_from_values

        all_values = {}
        for value_id in self.value_ids:

            value = self.kiara_context.data_registry.get_value(value_id)
            all_values[str(value_id)] = value
        table = create_renderable_from_values(values=all_values, config=config)

        return table


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
#     def _persist_value_in_session(self, value: Value, session: Session) -> Value:
#
#         if value.data is None:
#             raise NotImplementedError()
#
#         persisted = self._persist_value_data(value)
#         return None
#
#         data_type: DataType = self._kiara.get_value_type(
#             data_type=value.value_schema.type, data_type_config=value.value_schema.type_config
#         )
#
#         size = value.value_size
#         hash = value.value_hash
#
#         value_type_orm = (
#             session.query(ValueTypeOrm)
#             .filter_by(
#                 type_config_hash=data_type.value_type_hash,
#                 type_name=data_type.data_type_name,
#             )
#             .first()
#         )
#         if value_type_orm is None:
#             value_type_orm = ValueTypeOrm(
#                 type_config_hash=data_type.value_type_hash,
#                 type_name=data_type.data_type_name,
#                 type_config=data_type.type_config.dict(),
#             )
#             session.add(value_type_orm)
#             session.commit()
#
#         value_orm = (
#             session.query(Value)
#             .filter_by(
#                 value_hash=hash,
#                 value_size=size,
#                 data_type_name=data_type.data_type_name,
#             )
#             .first()
#         )
#         if value_orm is None:
#             value_id = uuid.uuid4()
#             value_orm = Value(
#                 global_id=value_id,
#                 data_type_name=data_type.data_type_name,
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
#                     module_config=module.render_config.dict(),
#                     is_idempotent=module.is_idempotent,
#                 )
#                 session.add(mc)
#                 session.commit()
#
#         return m
