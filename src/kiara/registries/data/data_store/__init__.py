# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import structlog
import uuid
from rich.console import RenderableType
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Set, Union

from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.matchers import ValueMatcher
from kiara.models.values.value import PersistedData, Value, ValuePedigree
from kiara.models.values.value_schema import ValueSchema
from kiara.registries import ARCHIVE_CONFIG_CLS, BaseArchive

if TYPE_CHECKING:
    pass

logger = structlog.getLogger()


class DataArchive(BaseArchive):
    @classmethod
    def supported_item_types(cls) -> Iterable[str]:

        return ["data"]

    def __init__(self, archive_id: uuid.UUID, config: ARCHIVE_CONFIG_CLS):

        super().__init__(archive_id=archive_id, config=config)

        self._env_cache: Dict[str, Dict[str, Mapping[str, Any]]] = {}
        self._value_cache: Dict[uuid.UUID, Value] = {}
        self._persisted_value_cache: Dict[uuid.UUID, PersistedData] = {}
        self._value_hash_index: Dict[str, Set[uuid.UUID]] = {}

    def retrieve_serialized_value(
        self, value: Union[uuid.UUID, Value]
    ) -> PersistedData:

        if isinstance(value, Value):
            value_id: uuid.UUID = value.value_id
            _value: Union[Value, None] = value
        else:
            value_id = value
            _value = None

        if value_id in self._persisted_value_cache.keys():
            return self._persisted_value_cache[value_id]

        if _value is None:
            _value = self.retrieve_value(value_id)

        assert _value is not None

        persisted_value = self._retrieve_serialized_value(value=_value)
        self._persisted_value_cache[_value.value_id] = persisted_value
        return persisted_value

    @abc.abstractmethod
    def _retrieve_serialized_value(self, value: Value) -> PersistedData:
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
            environment_hashes=value_data.get("environment_hashes", {}),
            pedigree=pedigree,
            pedigree_output_name=value_data["pedigree_output_name"],
            data_type_info=value_data["data_type_info"],
            property_links=value_data["property_links"],
            destiny_backlinks=value_data["destiny_backlinks"],
        )

        self._value_cache[value_id] = value
        return self._value_cache[value_id]

    @abc.abstractmethod
    def _retrieve_value_details(self, value_id: uuid.UUID) -> Mapping[str, Any]:
        pass

    @property
    def value_ids(self) -> Union[None, Iterable[uuid.UUID]]:
        return self._retrieve_all_value_ids()

    def _retrieve_all_value_ids(
        self, data_type_name: Union[str, None] = None
    ) -> Union[None, Iterable[uuid.UUID]]:
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

        all_value_ids = self.value_ids
        if all_value_ids is None:
            return False
        return value_id in all_value_ids

    def retrieve_environment_details(
        self, env_type: str, env_hash: str
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
        self, env_type: str, env_hash: str
    ) -> Mapping[str, Any]:
        pass

    def find_values(self, matcher: ValueMatcher) -> Iterable[Value]:
        raise NotImplementedError()

    def find_values_with_hash(
        self,
        value_hash: str,
        value_size: Union[int, None] = None,
        data_type_name: Union[str, None] = None,
    ) -> Set[uuid.UUID]:

        if data_type_name is not None:
            raise NotImplementedError()

        if value_size is not None:
            raise NotImplementedError()

        if value_hash in self._value_hash_index.keys():
            value_ids: Union[Set[uuid.UUID], None] = self._value_hash_index[value_hash]
        else:
            value_ids = self._find_values_with_hash(
                value_hash=value_hash, data_type_name=data_type_name
            )
            if value_ids is None:
                value_ids = set()
            self._value_hash_index[value_hash] = value_ids

        assert value_ids is not None
        return value_ids

    @abc.abstractmethod
    def _find_values_with_hash(
        self,
        value_hash: str,
        value_size: Union[int, None] = None,
        data_type_name: Union[str, None] = None,
    ) -> Union[Set[uuid.UUID], None]:
        pass

    def find_destinies_for_value(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Union[Mapping[str, uuid.UUID], None]:

        return self._find_destinies_for_value(
            value_id=value_id, alias_filter=alias_filter
        )

    @abc.abstractmethod
    def _find_destinies_for_value(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Union[Mapping[str, uuid.UUID], None]:
        pass

    @abc.abstractmethod
    def retrieve_chunk(
        self,
        chunk_id: str,
        as_file: Union[bool, str, None] = None,
        symlink_ok: bool = True,
    ) -> Union[bytes, str]:
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
    def store_value(self, value: Value) -> PersistedData:
        """ "Store the value, its data and metadata into the store.

        Arguments:
            value: the value to persist

        Returns:
            the load config that is needed to retrieve the value data later
        """


class BaseDataStore(DataStore):
    # @abc.abstractmethod
    # def _persist_bytes(self, bytes_structure: BytesStructure) -> BytesAliasStructure:
    #     pass

    @abc.abstractmethod
    def _persist_stored_value_info(self, value: Value, persisted_value: PersistedData):
        pass

    @abc.abstractmethod
    def _persist_value_details(self, value: Value):
        pass

    @abc.abstractmethod
    def _persist_value_data(self, value: Value) -> PersistedData:
        pass

    @abc.abstractmethod
    def _persist_value_pedigree(self, value: Value):
        """Create an internal link from a value to its pedigree (and pedigree details).

        This is so that the 'retrieve_job_record' can be used to prevent running the same job again, and the link of value
        to the job that produced it is preserved.
        """

    @abc.abstractmethod
    def _persist_environment_details(
        self, env_type: str, env_hash: str, env_data: Mapping[str, Any]
    ):
        pass

    @abc.abstractmethod
    def _persist_destiny_backlinks(self, value: Value):
        pass

    def store_value(self, value: Value) -> PersistedData:

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

            env = self.kiara_context.environment_registry.get_environment_for_cid(
                env_hash
            )
            self.persist_environment(env)

        # save the value data and metadata
        persisted_value = self._persist_value(value)
        self._persisted_value_cache[value.value_id] = persisted_value
        self._value_cache[value.value_id] = value
        self._value_hash_index.setdefault(value.value_hash, set()).add(value.value_id)

        # now link the output values to the manifest
        # then, make sure the manifest is persisted
        self._persist_value_pedigree(value=value)

        return persisted_value

    def _persist_value(self, value: Value) -> PersistedData:

        # TODO: check if value id is already persisted?
        if value.is_set:
            persisted_value_info: PersistedData = self._persist_value_data(value=value)
            if not persisted_value_info:
                raise Exception(
                    "Can't write persisted value info, no load config returned when persisting value."
                )
            if not isinstance(persisted_value_info, PersistedData):
                raise Exception(
                    f"Can't write persisted value info, invalid result type '{type(persisted_value_info)}' when persisting value."
                )
        else:
            persisted_value_info = PersistedData(
                archive_id=self.archive_id,
                data_type=value.data_type_name,
                serialization_profile="none",
                data_type_config=value.data_type_config,
                chunk_id_map={},
            )

        self._persist_stored_value_info(
            value=value, persisted_value=persisted_value_info
        )
        self._persist_value_details(value=value)
        if value.destiny_backlinks:
            self._persist_destiny_backlinks(value=value)

        return persisted_value_info

    def persist_environment(self, environment: RuntimeEnvironment):
        """Persist the specified environment.

        The environment is stored as a dictionary, including it's schema, not as the actual Python model.
        This is to make sure it can still be loaded later on, in case the Python model has changed in later versions.
        """

        env_type = environment.get_environment_type_name()
        env_hash = str(environment.instance_cid)

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
        all_value_ids = self.value_ids
        if all_value_ids:
            for value_id in all_value_ids:

                value = self.kiara_context.data_registry.get_value(value_id)
                all_values[str(value_id)] = value
            table = create_renderable_from_values(values=all_values, config=config)

            return table
        else:
            return "Data archive does not support statically determined ids."


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
