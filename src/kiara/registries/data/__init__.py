# -*- coding: utf-8 -*-
import structlog
import uuid
from rich.console import RenderableType
from sqlalchemy.engine import Engine
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Type,
)

from kiara.defaults import (
    INVALID_HASH_MARKER,
    NONE_VALUE_ID,
    NOT_SET_VALUE_ID,
    ORPHAN_PEDIGREE_OUTPUT_NAME,
    STRICT_CHECKS,
    SpecialValue,
)
from kiara.exceptions import InvalidValuesException, JobConfigException
from kiara.models.events import KiaraEvent
from kiara.models.events.data_registry import (
    DataStoreAddedEvent,
    ValueCreatedEvent,
    ValuePreStoreEvent,
    ValueStoredEvent,
)
from kiara.models.module.manifest import LoadConfig
from kiara.models.python_class import PythonClass
from kiara.models.values import ValueStatus
from kiara.models.values.value import (
    ORPHAN,
    UnloadableData,
    Value,
    ValuePedigree,
    ValueSet,
    ValueSetReadOnly,
)
from kiara.models.values.value_schema import ValueSchema
from kiara.modules.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)
from kiara.registries.data.data_store import DataArchive, DataStore
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import is_debug, log_message

if TYPE_CHECKING:
    from kiara.kiara import Kiara

logger = structlog.getLogger()


class DataRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._engine: Engine = self._kiara._engine

        self._event_callback: Callable = self._kiara.event_registry.add_producer(self)

        self._data_stores: Dict[str, DataStore] = {}

        self._default_data_store: Optional[str] = None

        # self.register_data_archive(FilesystemDataStore(kiara=self._kiara), alias=DEFAULT_STORE_MARKER)

        self._registered_values: Dict[uuid.UUID, Value] = {}

        self._value_store_map: Dict[uuid.UUID, str] = {}
        # self._job_store_map: Dict[int, uuid.UUID] = {}

        self._values_by_hash: Dict[int, Set[uuid.UUID]] = {}

        self._cached_data: Dict[uuid.UUID, Any] = {}
        self._load_configs: Dict[uuid.UUID, Optional[LoadConfig]] = {}

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

    def suppoerted_event_types(self) -> Iterable[Type[KiaraEvent]]:

        return [
            DataStoreAddedEvent,
            ValuePreStoreEvent,
            ValueStoredEvent,
            ValueCreatedEvent,
        ]

    # @property
    # def aliases(self) -> AliasRegistry:
    #
    #     if self._alias_registry is not None:
    #         return self._alias_registry
    #
    #     root_doc = "The root for all value aliases."
    #     self._alias_registry = AliasRegistry()
    #     self._alias_registry._data_registry = self
    #     self._alias_registry._engine = self._engine
    #
    #     return self._alias_registry
    #
    # def register_alias(self, alias: str, value: Union[Value, uuid.UUID]):
    #
    #     value = self.get_value(value=value)
    #     self.aliases.set_alias(alias=alias, value_id=value.value_id)
    #     self.aliases.save(alias)

    def register_data_archive(self, data_store: DataStore, alias: str = None):

        data_store_id = data_store.register_archive(kiara=self._kiara)
        if alias is None:
            alias = str(data_store_id)

        if alias in self._data_stores.keys():
            raise Exception(f"Can't add store, alias '{alias}' already registered.")
        self._data_stores[alias] = data_store
        is_store = False
        is_default_store = False
        if isinstance(data_store, DataStore):
            is_store = True
            if self._default_data_store is None:
                is_default_store = True
                self._default_data_store = alias

        event = DataStoreAddedEvent.construct(
            kiara_id=self._kiara.id,
            data_archive_id=data_store.data_store_id,
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
    def data_stores(self) -> Mapping[str, DataStore]:
        return self._data_stores

    def get_archive(self, store_id: Optional[str] = None) -> DataArchive:
        if store_id is None:
            store_id = self.default_data_store
            if store_id is None:
                raise Exception("Can't retrieve deafult data archive, none set (yet).")

        return self._data_stores[store_id]

    # def add_hook(self, hook: DataEventHook):
    #
    #     event_types = hook.get_subscribed_event_types()
    #     if isinstance(event_types, str):
    #         event_types = [event_types]
    #     for event_type in event_types:
    #         self._event_hooks.setdefault(event_type, []).append(hook)

    def find_store_id_for_value(self, value_id: uuid.UUID) -> Optional[str]:

        if value_id in self._value_store_map.keys():
            return self._value_store_map[value_id]

        matches = []
        for store_id, store in self.data_stores.items():
            match = store.has_value(value_id=value_id)
            if match:
                matches.append(store_id)

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Found value with id '{value_id}' in multiple archives, this is not supported (yet): {matches}"
            )

        self._value_store_map[value_id] = matches[0]
        return matches[0]

    def get_value(self, value_id: uuid.UUID) -> Value:

        if not isinstance(value_id, uuid.UUID):
            # fallbacks for common mistakes, this should error out if not a Value or string.
            if hasattr(value_id, "value_id"):
                value_id = value_id.value_id
            else:
                value_id = uuid.UUID(value_id)

        if value_id in self._registered_values.keys():
            return self._registered_values[value_id]

        matches = []
        for store_id, store in self.data_stores.items():
            match = store.has_value(value_id=value_id)
            if match:
                matches.append(store_id)

        if len(matches) == 0:
            raise Exception(f"No value registered with id: {value_id}")
        elif len(matches) > 1:
            raise Exception(
                f"Found value with id '{value_id}' in multiple archives, this is not supported (yet): {matches}"
            )

        self._value_store_map[value_id] = matches[0]
        stored_value = self.get_archive(matches[0]).retrieve_value(value_id=value_id)
        stored_value._set_registry(self)
        stored_value._is_stored = True

        self._registered_values[value_id] = stored_value
        return self._registered_values[value_id]

    def store_value(
        self,
        value: Value,
        store_id: Optional[str] = None,
        skip_if_exists: bool = True,
    ):

        if store_id is None:
            store_id = self.default_data_store

        store: DataStore = self.get_archive(store_id=store_id)  # type: ignore
        if not isinstance(store, DataStore):
            raise Exception(f"Can't store value into store '{store_id}': not writable.")

        # make sure all property values are available

        if not store.has_value(value.value_id) or not skip_if_exists:
            event = ValuePreStoreEvent.construct(kiara_id=self._kiara.id, value=value)
            self._event_callback(event)
            load_config = store.store_value(value)
            value._is_stored = True
            self._value_store_map[value.value_id] = store_id
            self._load_configs[value.value_id] = load_config
            property_values = value.property_values

            for property, property_value in property_values.items():
                self.store_value(
                    value=property_value, store_id=store_id, skip_if_exists=True
                )

        store_event = ValueStoredEvent.construct(kiara_id=self._kiara.id, value=value)
        self._event_callback(store_event)

    def find_values_for_hash(
        self, value_hash: int, data_type_name: Optional[str] = None
    ) -> Set[Value]:

        if data_type_name:
            raise NotImplementedError()

        stored = self._values_by_hash.get(value_hash, None)
        if stored is None:
            matches: Dict[uuid.UUID, List[str]] = {}
            for store_id, store in self.data_stores.items():
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
                self._value_store_map[v_id] = store_ids[0]
                stored.add(v_id)

            if stored:
                self._values_by_hash[value_hash] = stored

        return set((self.get_value(value_id=v_id) for v_id in stored))

    def register_data(
        self,
        data: Any,
        schema: Optional[ValueSchema] = None,
        pedigree: Optional[ValuePedigree] = None,
        pedigree_output_name: str = None,
        reuse_existing: bool = True,
    ) -> Value:

        value = self._create_value(
            data=data,
            schema=schema,
            pedigree=pedigree,
            pedigree_output_name=pedigree_output_name,
            reuse_existing=reuse_existing,
        )
        self._values_by_hash.setdefault(value.value_hash, set()).add(value.value_id)
        self._registered_values[value.value_id] = value
        self._cached_data[value.value_id] = data

        return value

    def _create_value(
        self,
        data: Any,
        schema: Optional[ValueSchema] = None,
        pedigree: Optional[ValuePedigree] = None,
        pedigree_output_name: str = None,
        reuse_existing: bool = True,
        value_id: Optional[uuid.UUID] = None,
    ) -> Value:

        if schema is None:
            raise NotImplementedError()

        if pedigree is None:
            raise NotImplementedError()

        if reuse_existing and value_id:
            raise Exception(
                f"Can't create value with pre-registered id '{value_id}': 'reuse_existing' set to True, which is not allowed if 'value_id' is set."
            )

        if pedigree_output_name is None:
            if pedigree == ORPHAN:
                pedigree_output_name = ORPHAN_PEDIGREE_OUTPUT_NAME
            else:
                raise NotImplementedError()

        if schema.type not in self._kiara.data_type_names:
            raise Exception(
                f"Can't register data of type '{schema.type}': type not registered. Available types: {', '.join(self._kiara.data_type_names)}"
            )

        if isinstance(data, str) and data.startswith("value:"):
            data = uuid.UUID(data[6:])

        if isinstance(data, uuid.UUID):
            data = self.get_value(value_id=data)

        if isinstance(data, Value):

            if data.value_id in self._registered_values.keys():
                if reuse_existing:
                    return data
                else:
                    raise Exception(
                        f"Can't register value '{data.value_id}: already registered"
                    )

            raise NotImplementedError()
            self._registered_values[data.value_id] = data
            return data

        data_type = self._kiara.type_registry.retrieve_data_type(
            data_type_name=schema.type, data_type_config=schema.type_config
        )

        if data == SpecialValue.NOT_SET:
            status = ValueStatus.NOT_SET
            value_hash = INVALID_HASH_MARKER
        elif data == SpecialValue.NO_VALUE:
            status = ValueStatus.NONE
            value_hash = INVALID_HASH_MARKER
        else:
            data, status, value_hash = data_type._pre_examine_data(
                data=data, schema=schema
            )

        existing_value: Optional[Value] = None
        if reuse_existing and value_hash != INVALID_HASH_MARKER:
            existing = self.find_values_for_hash(value_hash=value_hash)
            if existing:
                if len(existing) == 1:
                    existing_value = next(iter(existing))
                else:
                    skalars = []
                    for v in existing:
                        if v.data_type.characteristics.is_skalar:
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
            self._load_configs[existing_value.value_id] = None
            return existing_value

        if value_id:
            v_id = value_id
        else:
            v_id = ID_REGISTRY.generate(
                type="value", kiara_id=self._kiara.id, pre_registered=False
            )
        value, data = data_type.assemble_value(
            value_id=v_id,
            data=data,
            schema=schema,
            status=status,
            value_hash=value_hash,
            pedigree=pedigree,
            kiara_id=self._kiara.id,
            pedigree_output_name=pedigree_output_name,
        )
        ID_REGISTRY.update_metadata(v_id, obj=value)
        value._data_registry = self

        event = ValueCreatedEvent(kiara_id=self._kiara.id, value=value)
        self._event_callback(event)

        return value

    def retrieve_load_config(self, value_id: uuid.UUID) -> Optional[LoadConfig]:

        if (
            value_id in self._load_configs.keys()
            and self._load_configs[value_id] is not None
        ):
            load_config = self._load_configs[value_id]
        else:
            # now, the value_store map should contain this value_id
            store_id = self.find_store_id_for_value(value_id=value_id)
            if store_id is None:
                return None

            store = self.get_archive(store_id)
            self.get_value(value_id=value_id)
            load_config = store.retrieve_load_config(value=value_id)
            self._load_configs[value_id] = load_config

        return load_config

    def retrieve_value_data(self, value_id: uuid.UUID) -> Any:

        if value_id in self._cached_data.keys():
            return self._cached_data[value_id]

        load_config = self.retrieve_load_config(value_id=value_id)

        if load_config is None:
            raise Exception(
                f"Load config for value '{value_id}' is 'None', this is most likely a bug."
            )

        data = self._load_data_from_load_config(
            load_config=load_config, value_id=value_id
        )
        self._cached_data[value_id] = data

        return data

    def _load_data_from_load_config(
        self, load_config: LoadConfig, value_id: uuid.UUID
    ) -> Any:

        logger.debug("value.load", module=load_config.module_type)

        # TODO: check whether modules and value types are available

        try:
            job_config = self._kiara.job_registry.prepare_job_config(
                manifest=load_config, inputs=load_config.inputs
            )
        except JobConfigException:
            if is_debug():
                import traceback

                traceback.print_exc()
            value = self.get_value(value_id=value_id)
            return UnloadableData(value=value, load_config=load_config)

        job_id = self._kiara.job_registry.execute_job(job_config=job_config)
        result = self._kiara.job_registry.retrieve_result(job_id=job_id)
        # data = result.get_value_data(load_config.output_name)
        result_value = result.get_value_obj(field_name=load_config.output_name)

        return result_value.data

    def load_values(self, values: Mapping[str, Optional[uuid.UUID]]) -> ValueSet:

        value_items = {}
        schemas = {}
        for field_name, value_id in values.items():
            if value_id is None:
                value_id = NONE_VALUE_ID
            value = self.get_value(value_id=value_id)
            value_items[field_name] = value
            schemas[field_name] = value.value_schema

        return ValueSetReadOnly(value_items=value_items, values_schema=schemas)

    def load_data(self, values: Mapping[str, Optional[uuid.UUID]]) -> Mapping[str, Any]:

        result_values = self.load_values(values=values)
        return {k: v.data for k, v in result_values.items()}

    def create_valueset(
        self, data: Mapping[str, Any], schema: Mapping[str, ValueSchema]
    ) -> ValueSet:
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
            elif data[input_name] == None:
                value_data = SpecialValue.NO_VALUE
            else:
                value_data = data[input_name]
            try:
                value = self.retrieve_or_create_value(
                    value_data, value_schema=value_schema
                )
                values[input_name] = value
            except Exception as e:
                if is_debug():
                    import traceback

                    traceback.print_exc()
                failed[input_name] = e

        if failed:
            msg = []
            for k, v in failed.items():
                if not str(v):
                    v = str(type(v).__name__)
                msg.append(f"{k}: {v}")
            raise InvalidValuesException(
                msg=f"Can't create values instance: {', '.join(msg)}",
                invalid_values={k: str(v) for k, v in failed.items()},
            )

        return ValueSetReadOnly(value_items=values, values_schema=schema)  # type: ignore

    def retrieve_or_create_value(
        self, value_or_data: Any, value_schema: ValueSchema
    ) -> Value:

        if isinstance(value_or_data, Value):
            if value_or_data.value_id in self._registered_values.keys():
                existing = self._registered_values[value_or_data.value_id]
                if existing == value_or_data:
                    return existing

            raise NotImplementedError()

        elif isinstance(value_or_data, uuid.UUID):
            existing = self.get_value(value_or_data)
            # TODO: maybe check whether value is correct type or subtype?
            return existing
        elif isinstance(value_or_data, str):
            valid_uuid = False
            try:
                v_id = uuid.UUID(value_or_data)
                valid_uuid = True
                existing = self.get_value(v_id)
                # TODO: maybe check whether value is correct type or subtype?
                return existing
            except Exception as e:
                pass

            if valid_uuid:
                raise NotImplementedError()

        value = self.register_data(
            data=value_or_data,
            schema=value_schema,
            pedigree=ORPHAN,
            pedigree_output_name="__void__",
        )
        return value

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

        value = self.get_value(value_id=value_id)

        op_type: RenderValueOperationType = self._kiara.operation_registry.get_operation_type("render_value")  # type: ignore
        try:
            op = op_type.get_operation_for_render_combination(
                source_type=value.value_schema.type, target_type=target_type
            )
        except Exception:
            op = None
            if target_type == "terminal_renderable":
                try:
                    op = op_type.get_operation_for_render_combination(
                        source_type="any", target_type="string"
                    )
                except Exception:
                    pass
            if op is None:
                raise Exception(
                    f"Can't find operation to render '{value.value_schema.type}' as '{target_type}."
                )

        assert op is not None
        result = op.run(kiara=self._kiara, inputs={"value": value})
        rendered = result.get_value_data("rendered_value")
        return rendered
