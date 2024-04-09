# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import typing
import uuid
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    Mapping,
    Sequence,
    Set,
    Union,
)

import structlog
from rich.console import RenderableType

from kiara.models.values.matchers import ValueMatcher
from kiara.models.values.value import (
    SERIALIZE_TYPES,
    PersistedData,
    SerializedChunkIDs,
    SerializedData,
    Value,
    ValuePedigree,
)
from kiara.models.values.value_schema import ValueSchema
from kiara.registries import ARCHIVE_CONFIG_CLS, BaseArchive
from kiara.utils.dates import get_earliest_time_incl_timezone

if TYPE_CHECKING:
    from multiformats import CID
    from multiformats.varint import BytesLike

logger = structlog.getLogger()


class DataArchive(BaseArchive[ARCHIVE_CONFIG_CLS], typing.Generic[ARCHIVE_CONFIG_CLS]):
    """Base class for data archiv implementationss."""

    @classmethod
    def supported_item_types(cls) -> Iterable[str]:
        """This archive type only supports storing data."""

        return ["data"]

    def __init__(
        self,
        archive_name: str,
        archive_config: ARCHIVE_CONFIG_CLS,
        force_read_only: bool = False,
    ):

        super().__init__(
            archive_name=archive_name,
            archive_config=archive_config,
            force_read_only=force_read_only,
        )

        self._env_cache: Dict[str, Dict[str, Mapping[str, Any]]] = {}
        self._value_cache: Dict[uuid.UUID, Value] = {}
        self._persisted_value_cache: Dict[uuid.UUID, PersistedData] = {}
        self._value_hash_index: Dict[str, Set[uuid.UUID]] = {}

    def retrieve_serialized_value(
        self, value: Union[uuid.UUID, Value]
    ) -> PersistedData:
        """Retrieve a 'PersistedData' instance from a value id or value instance."""

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
        """Retrieve a 'PersistedData' instance from a value instance.

        This method basically implements the store-specific logic to serialize/deserialize the value data to/from disk.

        Raise an exception if the value is not persisted in this archive.
        """

    def retrieve_value(self, value_id: uuid.UUID) -> Value:
        """Retrieve the value for the specified value_id.

        Looks up the value in the cache first, and if not found, calls the '_retrieve_value_details' method to retrieve

        Raises an exception if the value is not persisted in this archive.
        """

        cached = self._value_cache.get(value_id, None)
        if cached is not None:
            return cached

        value_data = self._retrieve_value_details(value_id=value_id)

        value_schema = ValueSchema(**value_data["value_schema"])
        # data_type = self._kiara.get_value_type(
        #         data_type=value_schema.type, data_type_config=value_schema.type_config
        #     )

        value_created = value_data.get("value_created", None)
        if value_created is None:
            value_created = get_earliest_time_incl_timezone()

        pedigree = ValuePedigree(**value_data["pedigree"])

        job_id_str = value_data.get("job_id", None)
        # TODO: check for this to be not-Null at some stage, once we can be sure it's always set (after release)
        if job_id_str is not None:
            job_id: Union[None, uuid.UUID] = uuid.UUID(job_id_str)
        else:
            job_id = None

        value = Value(
            value_id=value_data["value_id"],
            kiara_id=self.kiara_context.id,
            job_id=job_id,
            value_schema=value_schema,
            value_created=value_created,
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
        """Retrieve the value details for the specified value_id from disk.

        This method basically implements the store-specific logic to retrieve the value details from disk.

        """

    @property
    def value_ids(self) -> Union[None, Iterable[uuid.UUID]]:
        return self._retrieve_all_value_ids()

    @abc.abstractmethod
    def _retrieve_all_value_ids(
        self, data_type_name: Union[str, None] = None
    ) -> Union[None, Iterable[uuid.UUID]]:
        """Retrieve all value ids from the store.

        In the case that _retrieve_all_value_ids returns 'None', the store does not support statically determined value ids and the 'find_values' method(s) needs to be used to retrieve values. Also, 'has_value' can be used to test whether a specific value_id is stored in the archive.
        """

    def has_value(self, value_id: uuid.UUID) -> bool:
        """
        Check whether the specific value_id is persisted in this data store.

        Implementing classes are encouraged to override this method, and choose a suitable, implementation specific
        way to quickly determine whether a value id is valid for this data store.

        Arguments:
        ---------
            value_id: the id of the value to check.


        Returns:
        -------
            whether this data store contains the value with the specified id
        """
        all_value_ids = self.value_ids
        if all_value_ids is None:
            return False
        return value_id in all_value_ids

    # def retrieve_environment_details(
    #     self, env_type: str, env_hash: str
    # ) -> Mapping[str, Any]:
    #     """
    #     Retrieve the environment details with the specified type and hash.
    #
    #     The environment is stored by the data store as a dictionary, including it's schema, not as the actual Python model.
    #     This is to make sure it can still be loaded later on, in case the Python model has changed in later versions.
    #     """
    #     cached = self._env_cache.get(env_type, {}).get(env_hash, None)
    #     if cached is not None:
    #         return cached
    #
    #     env = self._retrieve_environment_details(env_type=env_type, env_hash=env_hash)
    #     self._env_cache.setdefault(env_type, {})[env_hash] = env
    #     return env
    #
    # @abc.abstractmethod
    # def _retrieve_environment_details(
    #     self, env_type: str, env_hash: str
    # ) -> Mapping[str, Any]:
    #     """Retrieve the environment details with the specified type and hash.
    #
    #     Each store needs to implement this so environemnt details related to a value can be retrieved later on. Since in most cases the environment details will not change, a lookup is more efficient than having to store the full information with each value.
    #     """

    def find_values(self, matcher: ValueMatcher) -> Iterable[Value]:
        raise NotImplementedError()

    def find_values_with_hash(
        self,
        value_hash: str,
        value_size: Union[int, None] = None,
        data_type_name: Union[str, None] = None,
    ) -> Set[uuid.UUID]:
        """Find all values that have data that match the specifid hash.

        If the data type name is specified, only values of that type are considered, which should speed up the search. Same with 'value_size'. But both filters are not implemented yet.
        """

        # if data_type_name is not None:
        #     raise NotImplementedError()
        #
        # if value_size is not None:
        #     raise NotImplementedError()

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

        # TODO: if data_type_name or value_size are specified, validate the results?

        return value_ids

    @abc.abstractmethod
    def _find_values_with_hash(
        self,
        value_hash: str,
        value_size: Union[int, None] = None,
        data_type_name: Union[str, None] = None,
    ) -> Union[Set[uuid.UUID], None]:
        """Find all values that have data that match the specifid hash.

        If the data type name is specified, only values of that type are considered, which should speed up the search. Same with 'value_size'.
        This needs to be implemented in the implementing store though, and might or might not be used.
        """

    def find_destinies_for_value(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Union[Mapping[str, uuid.UUID], None]:
        """Find all destinies for the specified value id.

        TODO: explain destinies, and when they would be used.

        For now, you can just return 'None' in your implementation.
        """

        return self._find_destinies_for_value(
            value_id=value_id, alias_filter=alias_filter
        )

    @abc.abstractmethod
    def _find_destinies_for_value(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Union[Mapping[str, uuid.UUID], None]:
        """Find all destinies for the specified value id.

        TODO: explain destinies, and when they would be used.

        For now, you can just return 'None' in your implementation.
        """

    # @abc.abstractmethod
    # def retrieve_chunk(
    #     self,
    #     chunk_id: str,
    #     as_file: Union[bool, None] = None,
    #     symlink_ok: bool = True,
    # ) -> Union["BytesLike", str]:
    #     """Retrieve the chunk with the specified id.
    #
    #     If 'as_file' is specified, the chunk is written to a file, and the file path is returned. Otherwise, the chunk is returned as 'bytes'.
    #     """

    @abc.abstractmethod
    def retrieve_chunks(
        self,
        chunk_ids: Sequence[str],
        as_files: bool = True,
        symlink_ok: bool = True,
    ) -> Generator[Union["BytesLike", str], None, None]:
        """Retrieve a generator with all the specified chunks.

        If 'as_files' is specified, the chunks are written to a file, and the file path is returned. Otherwise, the chunk is returned as 'bytes'.
        """


class DataStore(DataArchive):
    @classmethod
    def _is_writeable(cls) -> bool:
        return True

    @abc.abstractmethod
    def store_value(self, value: Value) -> PersistedData:
        """
        "Store the value, its data and metadata into the store.

        Arguments:
        ---------
            value: the value to persist

        Returns:
        -------
            the load config that is needed to retrieve the value data later
        """


class BaseDataStore(DataStore):
    @abc.abstractmethod
    def _persist_stored_value_info(self, value: Value, persisted_value: PersistedData):
        """Store the details about the persisted data.

        This is used so an archive of this type can load the value data again later on. Value metadata is stored separately, later, using the '_persist_value_details' method.
        """

    @abc.abstractmethod
    def _persist_value_details(self, value: Value):
        """Persist the value details.

        Important details are:
         - value_id
         - value_hash
         - value_size
         - data_type_name
         - value_metadata
        """

    @abc.abstractmethod
    def _persist_value_pedigree(self, value: Value):
        """
        Create an internal link from a value to its pedigree (and pedigree details).

        This is so that the 'retrieve_job_record' can be used to prevent running the same job again, and the link of value
        to the job that produced it is preserved.
        """

    # @abc.abstractmethod
    # def _persist_environment_details(
    #     self, env_type: str, env_hash: str, env_data: Mapping[str, Any]
    # ):
    #     """Persist the environment details.
    #
    #     Each store type needs to store this for lookup purposes.
    #     """

    @abc.abstractmethod
    def _persist_destiny_backlinks(self, value: Value):
        """Persist the destiny backlinks."""

    def store_value(self, value: Value) -> PersistedData:

        logger.debug(
            "store.value",
            data_type=value.value_schema.type,
            value_id=value.value_id,
            value_hash=value.value_hash,
        )

        # # first, persist environment information
        # for env_type, env_hash in value.pedigree.environments.items():
        #     cached = self._env_cache.get(env_type, {}).get(env_hash, None)
        #     if cached is not None:
        #         continue
        #
        #     env = self.kiara_context.environment_registry.get_environment_for_cid(
        #         env_hash
        #     )
        #     self.persist_environment(env)

        # save the value data and metadata
        persisted_value = self._persist_value(value)
        self._persisted_value_cache[value.value_id] = persisted_value
        self._value_cache[value.value_id] = value
        self._value_hash_index.setdefault(value.value_hash, set()).add(value.value_id)

        # now link the output values to the manifest
        # then, make sure the manifest is persisted
        self._persist_value_pedigree(value=value)

        return persisted_value

    @abc.abstractmethod
    def _persist_chunks(self, chunks: Mapping["CID", BytesIO]):
        """Persist the specified chunk, and return the chunk id.

        If the chunk is a string, it represents a local file path, otherwise it is a BytesIO instance representing the actual data of the chunk.
        """

    def _persist_value_data(self, value: Value) -> PersistedData:

        serialized_value: SerializedData = value.serialized_data

        # dbg(serialized_value.model_dump())

        SIZE_LIMIT = 100000000

        chunk_id_map = {}
        chunks_to_persist: Dict[CID, BytesIO] = {}
        chunks_persisted: Set[CID] = set()
        current_size = 0
        for key in serialized_value.get_keys():

            data_model = serialized_value.get_serialized_data(key)

            if data_model.type == "chunk":  # type: ignore
                chunks: Iterable[BytesIO] = [BytesIO(data_model.chunk)]  # type: ignore
            elif data_model.type == "chunks":  # type: ignore
                chunks = (BytesIO(c) for c in data_model.chunks)  # type: ignore
            elif data_model.type == "file":  # type: ignore
                chunks = [data_model.file]  # type: ignore
            elif data_model.type == "files":  # type: ignore
                chunks = data_model.files  # type: ignore
            elif data_model.type == "inline-json":  # type: ignore
                chunks = [BytesIO(data_model.as_json())]  # type: ignore
            elif data_model.type == "chunk-ids":  # type: ignore
                # means this is already serialized in a different store
                data_model_instance: SerializedChunkIDs = data_model  # type: ignore
                chunks = (
                    BytesIO(x) for x in data_model_instance.get_chunks(as_files=False)  # type: ignore
                )

            else:
                raise Exception(
                    f"Invalid serialized data type: {type(data_model)}. Available types: {', '.join(SERIALIZE_TYPES)}"
                )

            cids = serialized_value.get_cids_for_key(key)
            chunk_iterable = zip(cids, chunks)
            chunks_to_persist.update(chunk_iterable)

            chunk_ids = [str(cid) for cid in cids]
            scids = SerializedChunkIDs(
                chunk_id_list=chunk_ids,
                archive_id=self.archive_id,
                size=data_model.get_size(),
            )
            scids._data_registry = self.kiara_context.data_registry
            chunk_id_map[key] = scids

            key_size = data_model.get_size()
            current_size += key_size
            # this is not super-exact, because the actual size of all chunks to be persisted is not known
            # since some of them might be filtered out, should be good enough to not let the memory blow up too much
            if current_size > SIZE_LIMIT:
                self._persist_chunks(
                    chunks={
                        k: v
                        for k, v in chunks_to_persist.items()
                        if k not in chunks_persisted
                    }
                )
                chunks_persisted.update(chunks_to_persist.keys())
                chunks_to_persist = {}
                current_size = 0

        if chunks_to_persist:
            self._persist_chunks(
                chunks={
                    k: v
                    for k, v in chunks_to_persist.items()
                    if k not in chunks_persisted
                }
            )

        pers_value = PersistedData(
            archive_id=self.archive_id,
            chunk_id_map=chunk_id_map,
            data_type=serialized_value.data_type,
            data_type_config=serialized_value.data_type_config,
            serialization_profile=serialized_value.serialization_profile,
            metadata=serialized_value.metadata,
        )

        return pers_value

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
        # TODO: re-enable?
        if value.destiny_backlinks:
            self._persist_destiny_backlinks(value=value)

        return persisted_value_info

    # def persist_environment(self, environment: RuntimeEnvironment):
    #     """
    #     Persist the specified environment.
    #
    #     The environment is stored as a dictionary, including it's schema, not as the actual Python model.
    #     This is to make sure it can still be loaded later on, in case the Python model has changed in later versions.
    #     """
    #     env_type = environment.get_environment_type_name()
    #     env_hash = str(environment.instance_cid)
    #
    #     env = self._env_cache.get(env_type, {}).get(env_hash, None)
    #     if env is not None:
    #         return
    #
    #     env_data = environment.as_dict_with_schema()
    #     self._persist_environment_details(
    #         env_type=env_type, env_hash=env_hash, env_data=env_data
    #     )
    #     self._env_cache.setdefault(env_type, {})[env_hash] = env_data

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
