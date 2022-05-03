# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import structlog
import uuid
from rich.console import RenderableType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from kiara.data_types import DataType
from kiara.defaults import (
    INVALID_HASH_MARKER,
    NO_SERIALIZATION_MARKER,
    NONE_VALUE_ID,
    NOT_SET_VALUE_ID,
    ORPHAN_PEDIGREE_OUTPUT_NAME,
    STRICT_CHECKS,
    SpecialValue,
)
from kiara.exceptions import InvalidValuesException
from kiara.models.events.data_registry import (
    DataArchiveAddedEvent,
    ValueCreatedEvent,
    ValuePreStoreEvent,
    ValueRegisteredEvent,
    ValueStoredEvent,
)
from kiara.models.module.operation import Operation
from kiara.models.python_class import PythonClass
from kiara.models.values import ValueStatus
from kiara.models.values.value import (
    ORPHAN,
    PersistedData,
    SerializedData,
    Value,
    ValueMap,
    ValueMapReadOnly,
    ValuePedigree,
)
from kiara.models.values.value_schema import ValueSchema
from kiara.registries.data.data_store import DataArchive, DataStore
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import log_exception, log_message
from kiara.utils.data import render_data

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()


class DataRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

        self._event_callback: Callable = self._kiara.event_registry.add_producer(self)

        self._data_archives: Dict[str, DataArchive] = {}

        self._default_data_store: Optional[str] = None
        self._registered_values: Dict[uuid.UUID, Value] = {}

        self._value_archive_lookup_map: Dict[uuid.UUID, str] = {}

        self._values_by_hash: Dict[str, Set[uuid.UUID]] = {}

        self._cached_data: Dict[uuid.UUID, Any] = {}
        self._persisted_value_descs: Dict[uuid.UUID, Optional[PersistedData]] = {}

        # initialize special values
        special_value_cls = PythonClass.from_class(SpecialValue)
        self._not_set_value: Value = Value(
            value_id=NOT_SET_VALUE_ID,
            kiara_id=self._kiara.id,
            value_schema=ValueSchema(
                type="none",
                default=SpecialValue.NOT_SET,
                is_constant=True,
                doc="Special value, indicating a field is not set.",  # type: ignore
            ),
            value_status=ValueStatus.NOT_SET,
            value_size=0,
            value_hash=INVALID_HASH_MARKER,
            pedigree=ORPHAN,
            pedigree_output_name="__void__",
            data_type_class=special_value_cls,
        )
        self._cached_data[NOT_SET_VALUE_ID] = SpecialValue.NOT_SET

        self._none_value: Value = Value(
            value_id=NONE_VALUE_ID,
            kiara_id=self._kiara.id,
            value_schema=ValueSchema(
                type="special_type",
                default=SpecialValue.NO_VALUE,
                is_constant=True,
                doc="Special value, indicating a field is set with a 'none' value.",  # type: ignore
            ),
            value_status=ValueStatus.NONE,
            value_size=0,
            value_hash=-2,
            pedigree=ORPHAN,
            pedigree_output_name="__void__",
            data_type_class=special_value_cls,
        )
        self._cached_data[NONE_VALUE_ID] = SpecialValue.NO_VALUE

    @property
    def kiara_id(self) -> uuid.UUID:
        return self._kiara.id

    @property
    def NOT_SET_VALUE(self) -> Value:
        return self._not_set_value

    @property
    def NONE_VALUE(self) -> Value:
        return self._none_value

    def retrieve_all_available_value_ids(self) -> Set[uuid.UUID]:

        result: Set[uuid.UUID] = set()
        for store in self._data_archives.values():
            ids = store.value_ids
            result.update(ids)

        return result

    def register_data_archive(
        self,
        archive: DataArchive,
        alias: str = None,
        set_as_default_store: Optional[bool] = None,
    ):

        data_store_id = archive.register_archive(kiara=self._kiara)
        if alias is None:
            alias = str(data_store_id)

        if alias in self._data_archives.keys():
            raise Exception(f"Can't add store, alias '{alias}' already registered.")
        self._data_archives[alias] = archive
        is_store = False
        is_default_store = False
        if isinstance(archive, DataStore):
            is_store = True

            if set_as_default_store and self._default_data_store is not None:
                raise Exception(
                    f"Can't set data store '{alias}' as default store: default store already set."
                )

            if self._default_data_store is None or set_as_default_store:
                is_default_store = True
                self._default_data_store = alias

        event = DataArchiveAddedEvent.construct(
            kiara_id=self._kiara.id,
            data_archive_id=archive.archive_id,
            data_archive_alias=alias,
            is_store=is_store,
            is_default_store=is_default_store,
        )
        self._event_callback(event)

    @property
    def default_data_store(self) -> str:
        if self._default_data_store is None:
            raise Exception("No default data store set.")
        return self._default_data_store

    @property
    def data_archives(self) -> Mapping[str, DataArchive]:
        return self._data_archives

    def get_archive(
        self, archive_id: Union[None, uuid.UUID, str] = None
    ) -> DataArchive:

        if archive_id is None:
            archive_id = self.default_data_store
            if archive_id is None:
                raise Exception("Can't retrieve default data archive, none set (yet).")

        if isinstance(archive_id, uuid.UUID):
            for archive in self._data_archives.values():
                if archive.archive_id == archive_id:
                    return archive

            raise Exception(
                f"Can't retrieve archive with id '{archive_id}': no archive with that id registered."
            )

        if archive_id in self._data_archives.keys():
            return self._data_archives[archive_id]
        else:
            try:
                _archive_id = uuid.UUID(archive_id)
                for archive in self._data_archives.values():
                    if archive.archive_id == _archive_id:
                        return archive
                    raise Exception(
                        f"Can't retrieve archive with id '{archive_id}': no archive with that id registered."
                    )
            except Exception:
                pass

        raise Exception(
            f"Can't retrieve archive with id '{archive_id}': no archive with that id registered."
        )

    def find_store_id_for_value(self, value_id: uuid.UUID) -> Optional[str]:

        if value_id in self._value_archive_lookup_map.keys():
            return self._value_archive_lookup_map[value_id]

        matches = []
        for store_id, store in self.data_archives.items():
            match = store.has_value(value_id=value_id)
            if match:
                matches.append(store_id)

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Found value with id '{value_id}' in multiple archives, this is not supported (yet): {matches}"
            )

        self._value_archive_lookup_map[value_id] = matches[0]
        return matches[0]

    def get_value(self, value_id: Union[uuid.UUID, Value, str]) -> Value:
        _value_id = None
        if not isinstance(value_id, uuid.UUID):
            # fallbacks for common mistakes, this should error out if not a Value or string.
            if hasattr(value_id, "value_id"):
                _value_id: Optional[uuid.UUID] = value_id.value_id  # type: ignore
            else:

                try:
                    _value_id = uuid.UUID(
                        value_id  # type: ignore
                    )  # this should fail if not string or wrong string format
                except ValueError:
                    _value_id = None

                if _value_id is None:

                    if not isinstance(value_id, str):
                        raise Exception(
                            f"Can't retrieve value for '{value_id}': invalid type '{type(value_id)}'."
                        )

                    if ":" not in value_id:
                        raise Exception(
                            f"Can't retrieve value for '{value_id}': can't determine reference type."
                        )

                    ref_type, rest = value_id.split(":", maxsplit=1)

                    if ref_type == "value":
                        _value_id = uuid.UUID(rest)
                    elif ref_type == "alias":
                        _value_id = self._kiara.alias_registry.find_value_id_for_alias(
                            alias=rest
                        )
                        if _value_id is None:
                            raise Exception(
                                f"Can't retrive value for alias '{rest}': no such alias registered."
                            )
                    else:
                        raise Exception(
                            f"Can't retrieve value for '{value_id}': invalid reference type '{ref_type}'."
                        )
        else:
            _value_id = value_id

        assert _value_id is not None

        if _value_id in self._registered_values.keys():
            value = self._registered_values[_value_id]
            return value

        matches = []
        for store_id, store in self.data_archives.items():
            match = store.has_value(value_id=_value_id)
            if match:
                matches.append(store_id)

        if len(matches) == 0:
            raise Exception(f"No value registered with id: {value_id}")
        elif len(matches) > 1:
            raise Exception(
                f"Found value with id '{value_id}' in multiple archives, this is not supported (yet): {matches}"
            )

        self._value_archive_lookup_map[_value_id] = matches[0]
        stored_value = self.get_archive(matches[0]).retrieve_value(value_id=_value_id)
        stored_value._set_registry(self)
        stored_value._is_stored = True

        self._registered_values[_value_id] = stored_value
        return self._registered_values[_value_id]

    def store_value(
        self,
        value: Union[Value, uuid.UUID],
        store_id: Optional[str] = None,
    ) -> Optional[PersistedData]:

        if store_id is None:
            store_id = self.default_data_store

        if isinstance(value, uuid.UUID):
            value = self.get_value(value)

        store: DataStore = self.get_archive(archive_id=store_id)  # type: ignore
        if not isinstance(store, DataStore):
            raise Exception(f"Can't store value into store '{store_id}': not writable.")

        # make sure all property values are available
        if value.pedigree != ORPHAN:
            for value_id in value.pedigree.inputs.values():
                self.store_value(value=value_id, store_id=store_id)

        if not store.has_value(value.value_id):
            event = ValuePreStoreEvent.construct(kiara_id=self._kiara.id, value=value)
            self._event_callback(event)
            persisted_value = store.store_value(value)
            value._is_stored = True
            self._value_archive_lookup_map[value.value_id] = store_id
            self._persisted_value_descs[value.value_id] = persisted_value
            property_values = value.property_values

            for property, property_value in property_values.items():
                self.store_value(value=property_value, store_id=store_id)
        else:
            persisted_value = None

        store_event = ValueStoredEvent.construct(kiara_id=self._kiara.id, value=value)
        self._event_callback(store_event)

        return persisted_value

    def find_values_for_hash(
        self, value_hash: str, data_type_name: Optional[str] = None
    ) -> Set[Value]:

        if data_type_name:
            raise NotImplementedError()

        stored = self._values_by_hash.get(value_hash, None)
        if stored is None:
            matches: Dict[uuid.UUID, List[str]] = {}
            for store_id, store in self.data_archives.items():
                value_ids = store.find_values_with_hash(
                    value_hash=value_hash, data_type_name=data_type_name
                )
                for v_id in value_ids:
                    matches.setdefault(v_id, []).append(store_id)

            stored = set()
            for v_id, store_ids in matches.items():
                if len(store_ids) > 1:
                    raise Exception(
                        f"Found multiple stores for value id '{v_id}', this is not supported (yet)."
                    )
                self._value_archive_lookup_map[v_id] = store_ids[0]
                stored.add(v_id)

            if stored:
                self._values_by_hash[value_hash] = stored

        return set((self.get_value(value_id=v_id) for v_id in stored))

    def find_destinies_for_value(
        self, value_id: uuid.UUID, alias_filter: str = None
    ) -> Mapping[str, uuid.UUID]:

        if alias_filter:
            raise NotImplementedError()

        all_destinies: Dict[str, uuid.UUID] = {}
        for archive_id, archive in self._data_archives.items():
            destinies: Optional[
                Mapping[str, uuid.UUID]
            ] = archive.find_destinies_for_value(
                value_id=value_id, alias_filter=alias_filter
            )
            if not destinies:
                continue
            for k, v in destinies.items():
                if k in all_destinies.keys():
                    raise Exception(f"Duplicate destiny '{k}' for value '{value_id}'.")
                all_destinies[k] = v

        return all_destinies

    def register_data(
        self,
        data: Any,
        schema: Union[ValueSchema, str] = None,
        pedigree: Optional[ValuePedigree] = None,
        pedigree_output_name: str = None,
        reuse_existing: bool = True,
    ) -> Value:

        value, newly_created = self._create_value(
            data=data,
            schema=schema,
            pedigree=pedigree,
            pedigree_output_name=pedigree_output_name,
            reuse_existing=reuse_existing,
        )

        if newly_created:
            self._values_by_hash.setdefault(value.value_hash, set()).add(value.value_id)
            self._registered_values[value.value_id] = value
            self._cached_data[value.value_id] = data

            event = ValueRegisteredEvent(kiara_id=self._kiara.id, value=value)
            self._event_callback(event)

        return value

    def _find_existing_value(
        self, data: Any, schema: Optional[ValueSchema]
    ) -> Tuple[
        Optional[Value],
        DataType,
        Optional[Any],
        Union[str, SerializedData],
        ValueStatus,
        str,
        int,
    ]:

        if schema is None:
            raise NotImplementedError()

        if isinstance(data, Value):

            if data.value_id in self._registered_values.keys():
                if data.is_serializable:
                    serialized: Union[str, SerializedData] = data.serialized_data
                else:
                    serialized = NO_SERIALIZATION_MARKER
                return (
                    data,
                    data.data_type,
                    None,
                    serialized,
                    data.value_status,
                    data.value_hash,
                    data.value_size,
                )

            raise NotImplementedError("Importing values not supported (yet).")
            # self._registered_values[data.value_id] = data
            # return data

        try:
            value = self.get_value(value_id=data)
            if value.is_serializable:
                serialized = value.serialized_data
            else:
                serialized = NO_SERIALIZATION_MARKER

            return (
                value,
                value.data_type,
                None,
                serialized,
                value.value_status,
                value.value_hash,
                value.value_size,
            )
        except Exception:
            # TODO: differentiate between 'value not found' and other type of errors
            pass

        # no obvious matches, so we try to find data that has the same hash
        data_type = self._kiara.type_registry.retrieve_data_type(
            data_type_name=schema.type, data_type_config=schema.type_config
        )

        data, serialized, status, value_hash, value_size = data_type._pre_examine_data(
            data=data, schema=schema
        )

        existing_value: Optional[Value] = None
        if value_hash != INVALID_HASH_MARKER:
            existing = self.find_values_for_hash(value_hash=value_hash)
            if existing:
                if len(existing) == 1:
                    existing_value = next(iter(existing))
                else:
                    skalars = []
                    for v in existing:
                        if v.data_type.characteristics.is_scalar:
                            skalars.append(v)

                    if len(skalars) == 1:
                        existing_value = skalars[0]
                    elif skalars:
                        orphans = []
                        for v in skalars:
                            if v.pedigree == ORPHAN:
                                orphans.append(v)

                        if len(orphans) == 1:
                            existing_value = orphans[0]

        if existing_value is not None:
            self._persisted_value_descs[existing_value.value_id] = None
            return (
                existing_value,
                data_type,
                data,
                serialized,
                status,
                value_hash,
                value_size,
            )

        return (None, data_type, data, serialized, status, value_hash, value_size)

    def _create_value(
        self,
        data: Any,
        schema: Union[None, str, ValueSchema] = None,
        pedigree: Optional[ValuePedigree] = None,
        pedigree_output_name: str = None,
        reuse_existing: bool = True,
    ) -> Tuple[Value, bool]:
        """Create a new value, or return an existing one that matches the incoming data or reference.

        Arguments:
            data: the (raw) data, or a reference to an existing value


        Returns:
            a tuple containing of the value object, and a boolean indicating whether the value was newly created (True), or already existing (False)
        """

        if schema is None:
            raise NotImplementedError()
        elif isinstance(schema, str):
            schema = ValueSchema(type=schema)

        if schema.type not in self._kiara.data_type_names:
            raise Exception(
                f"Can't register data of type '{schema.type}': type not registered. Available types: {', '.join(self._kiara.data_type_names)}"
            )

        # data_type: Optional[DataType] = None
        # status: Optional[ValueStatus] = None
        # value_hash: Optional[str] = None

        if reuse_existing:
            (
                _existing,
                data_type,
                data,
                serialized,
                status,
                value_hash,
                value_size,
            ) = self._find_existing_value(data=data, schema=schema)
            if _existing is not None:
                # TODO: check pedigree
                return (_existing, False)
        else:

            data_type = self._kiara.type_registry.retrieve_data_type(
                data_type_name=schema.type, data_type_config=schema.type_config
            )

            (
                data,
                serialized,
                status,
                value_hash,
                value_size,
            ) = data_type._pre_examine_data(data=data, schema=schema)

        if pedigree is None:
            pedigree = ORPHAN

        if pedigree_output_name is None:
            if pedigree == ORPHAN:
                pedigree_output_name = ORPHAN_PEDIGREE_OUTPUT_NAME
            else:
                raise NotImplementedError()

        v_id = ID_REGISTRY.generate(
            type="value", kiara_id=self._kiara.id, pre_registered=False
        )

        value, data = data_type.assemble_value(
            value_id=v_id,
            data=data,
            schema=schema,
            serialized=serialized,
            status=status,
            value_hash=value_hash,
            value_size=value_size,
            pedigree=pedigree,
            kiara_id=self._kiara.id,
            pedigree_output_name=pedigree_output_name,
        )
        ID_REGISTRY.update_metadata(v_id, obj=value)
        value._data_registry = self

        event = ValueCreatedEvent(kiara_id=self._kiara.id, value=value)
        self._event_callback(event)

        return (value, True)

    def retrieve_persisted_value_details(self, value_id: uuid.UUID) -> PersistedData:

        if (
            value_id in self._persisted_value_descs.keys()
            and self._persisted_value_descs[value_id] is not None
        ):
            persisted_details = self._persisted_value_descs[value_id]
            assert persisted_details is not None
        else:
            # now, the value_store map should contain this value_id
            store_id = self.find_store_id_for_value(value_id=value_id)
            if store_id is None:
                raise Exception(
                    f"Can't find store for persisted data of value: {value_id}"
                )

            store = self.get_archive(store_id)
            assert value_id in self._registered_values.keys()
            # self.get_value(value_id=value_id)
            persisted_details = store.retrieve_serialized_value(value=value_id)
            for c in persisted_details.chunk_id_map.values():
                c._data_registry = self._kiara.data_registry
            self._persisted_value_descs[value_id] = persisted_details

        return persisted_details

    # def _retrieve_bytes(
    #     self, chunk_id: str, as_link: bool = True
    # ) -> Union[str, bytes]:
    #
    #     # TODO: support multiple stores
    #     return self.get_archive().retrieve_chunk(chunk_id=chunk_id, as_link=as_link)

    def retrieve_serialized_value(
        self, value_id: uuid.UUID
    ) -> Optional[SerializedData]:
        """Create a LoadConfig object from the details of the persisted version of this value."""

        pv = self.retrieve_persisted_value_details(value_id=value_id)
        if pv is None:
            return None

        return pv

    def retrieve_chunk(
        self,
        chunk_id: str,
        archive_id: Optional[uuid.UUID] = None,
        as_file: Union[None, bool, str] = None,
        symlink_ok: bool = True,
    ) -> Union[str, bytes]:

        if archive_id is None:
            raise NotImplementedError()

        archive = self.get_archive(archive_id)
        chunk = archive.retrieve_chunk(chunk_id, as_file=as_file, symlink_ok=symlink_ok)

        return chunk

    def retrieve_value_data(
        self, value: Union[uuid.UUID, Value], target_profile: Optional[str] = None
    ) -> Any:

        if isinstance(value, uuid.UUID):
            value = self.get_value(value_id=value)

        if value.value_id in self._cached_data.keys():
            return self._cached_data[value.value_id]

        if value._serialized_data is None:
            serialized_data: Union[
                str, SerializedData
            ] = self.retrieve_persisted_value_details(value_id=value.value_id)
            value._serialized_data = serialized_data
        else:
            serialized_data = value._serialized_data

        if isinstance(serialized_data, str):
            raise Exception(
                f"Can't retrieve serialized version of value '{value.value_id}', this is most likely a bug."
            )

        manifest = serialized_data.metadata.deserialize.get("python_object", None)
        if manifest is None:
            raise Exception(
                f"No deserialize operation found for data type: {value.data_type_name}"
            )

        module = self._kiara.create_module(manifest=manifest)
        op = Operation.create_from_module(module=module)

        input_field_match: Optional[str] = None
        if len(op.inputs_schema) == 1:
            input_field_match = next(iter(op.inputs_schema.keys()))
        else:
            for input_field, schema in op.inputs_schema.items():
                if schema.type == value.data_type_name:
                    if input_field_match is not None:
                        raise Exception(
                            f"Can't determine input field for deserialization operation '{module.module_type_name}': multiple input fields with type '{input_field_match}'."
                        )
                    else:
                        input_field_match = input_field
        if input_field_match is None:
            raise Exception(
                f"Can't determine input field for deserialization operation '{module.module_type_name}'."
            )

        result_field_match: Optional[str] = None
        for result_field, schema in op.outputs_schema.items():
            if schema.type == "python_object":
                if result_field_match is not None:
                    raise Exception(
                        f"Can't determine result field for deserialization operation '{module.module_type_name}': multiple result fields with type 'python_object'."
                    )
                else:
                    result_field_match = result_field
        if result_field_match is None:
            raise Exception(
                f"Can't determine result field for deserialization operation '{module.module_type_name}'."
            )

        inputs = {input_field_match: value}

        result = op.run(kiara=self._kiara, inputs=inputs)
        python_object = result.get_value_data(result_field_match)
        self._cached_data[value.value_id] = python_object

        return python_object

        # op_type: DeSerializeOperationType = self._kiara.operation_registry.get_operation_type("deserialize")  # type: ignore
        # ops = op_type.find_deserialzation_operation_for_type_and_profile(
        #     serialized_data.data_type, serialized_data.serialization_profile
        # )
        #
        # if len(ops) > 1:
        #     raise Exception("No unique op.")
        #
        # if not ops:
        #     raise Exception(
        #         f"No deserialize operation found for data type: {value.data_type_name}"
        #     )
        #
        # op = ops[0]
        # inputs = {"value": serialized_data}
        #
        # result = op.run(kiara=self._kiara, inputs=inputs)
        #
        # python_object = result.get_value_data("python_object")
        # self._cached_data[value.value_id] = python_object
        #
        # return python_object

    def load_values(self, values: Mapping[str, Optional[uuid.UUID]]) -> ValueMap:

        value_items = {}
        schemas = {}
        for field_name, value_id in values.items():
            if value_id is None:
                value_id = NONE_VALUE_ID

            value = self.get_value(value_id=value_id)
            value_items[field_name] = value
            schemas[field_name] = value.value_schema

        return ValueMapReadOnly(value_items=value_items, values_schema=schemas)

    def load_data(self, values: Mapping[str, Optional[uuid.UUID]]) -> Mapping[str, Any]:

        result_values = self.load_values(values=values)
        return {k: v.data for k, v in result_values.items()}

    def create_valueset(
        self, data: Mapping[str, Any], schema: Mapping[str, ValueSchema]
    ) -> ValueMap:
        """Extract a set of [Value][kiara.data.values.Value] from Python data and ValueSchemas."""

        input_details = {}
        for input_name, value_schema in schema.items():
            input_details[input_name] = {"schema": value_schema}

        leftover = set(data.keys())
        leftover.difference_update(input_details.keys())
        if leftover:
            if not STRICT_CHECKS:
                log_message("unused.inputs", input_names=leftover)
            else:
                raise Exception(
                    f"Can't create values instance, inputs contain unused/invalid fields: {', '.join(leftover)}"
                )

        values = {}

        failed = {}
        for input_name, details in input_details.items():

            value_schema = details["schema"]

            if input_name not in data.keys():
                value_data = SpecialValue.NOT_SET
            elif data[input_name] is None:
                value_data = SpecialValue.NO_VALUE
            else:
                value_data = data[input_name]
            try:
                value = self.register_data(
                    data=value_data, schema=value_schema, reuse_existing=True
                )
                # value = self.retrieve_or_create_value(
                #     value_data, value_schema=value_schema
                # )
                values[input_name] = value
            except Exception as e:

                log_exception(e)

                msg: Any = str(e)
                if not msg:
                    msg = e
                log_message("invalid.valueset", error_reason=msg, input_name=input_name)
                failed[input_name] = e

        if failed:
            msg = []
            for k, v in failed.items():
                _v = str(v)
                if not str(v):
                    _v = type(v).__name__
                msg.append(f"{k}: {_v}")
            raise InvalidValuesException(
                msg=f"Can't create values instance: {', '.join(msg)}",
                invalid_values={k: str(v) for k, v in failed.items()},
            )

        return ValueMapReadOnly(value_items=values, values_schema=schema)  # type: ignore

    def create_renderable(self, **config: Any) -> RenderableType:
        """Create a renderable for this module configuration."""

        from kiara.utils.output import create_renderable_from_values

        all_values = {str(i): v for i, v in self._registered_values.items()}

        table = create_renderable_from_values(values=all_values, config=config)
        return table

    def render_data(
        self,
        value_id: uuid.UUID,
        target_type="terminal_renderable",
        **render_config: Any,
    ) -> Any:

        assert isinstance(value_id, uuid.UUID)

        return render_data(
            kiara=self._kiara,
            value_id=value_id,
            target_type=target_type,
            **render_config,
        )
