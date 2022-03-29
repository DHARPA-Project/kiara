# -*- coding: utf-8 -*-
import abc
import structlog
import uuid
from rich.console import RenderableType
from sqlalchemy.engine import Engine
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Union,
)

from kiara.defaults import (
    INVALID_HASH_MARKER,
    ORPHAN_PEDIGREE_OUTPUT_NAME,
    STRICT_CHECKS,
    SpecialValue,
)
from kiara.exceptions import JobConfigException
from kiara.kiara.aliases import AliasRegistry
from kiara.kiara.data_store import DataStore
from kiara.kiara.data_store.filesystem_store import FilesystemDataStore
from kiara.models.events.data_registry import (
    AliasPreStoreEvent,
    RegistryEvent,
    ValueCreatedEvent,
    ValuePreStoreEvent,
    ValueStoredEvent,
)
from kiara.models.module.destiniy import Destiny
from kiara.models.module.jobs import JobConfig, JobRecord
from kiara.models.module.manifest import LoadConfig, Manifest
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
from kiara.modules.operations.included_core_operations.metadata import (
    ExtractMetadataDetails,
    ExtractMetadataOperationType,
)
from kiara.modules.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)
from kiara.utils import is_debug, log_message

if TYPE_CHECKING:
    from kiara.kiara import Kiara

logger = structlog.getLogger()


class DataEventHook(abc.ABC):
    @abc.abstractmethod
    def get_subscribed_event_types(self) -> Iterable[str]:
        pass

    @abc.abstractmethod
    def process_hook(self, event: RegistryEvent):
        pass


class CreateMetadataDestinies(DataEventHook):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

    def get_subscribed_event_types(self) -> Iterable[str]:
        return ["value_created", "value_pre_store"]

    def process_hook(self, event: RegistryEvent):

        # if not value._data_type_known:
        #     return

        if event.event_type == "value_created":  # type: ignore
            self.attach_metadata(event.value)
        elif event.event_type == "value_pre_store":  # type: ignore
            self.resolve_all_metadata(event.value)

    def attach_metadata(self, value: Value):

        op_type: ExtractMetadataOperationType = self._kiara.operations_mgmt.get_operation_type("extract_metadata")  # type: ignore
        operations = op_type.get_operations_for_data_type(value.value_schema.type)
        for metadata_key, op in operations.items():
            op_details: ExtractMetadataDetails = op.operation_details  # type: ignore
            input_field_name = op_details.input_field_name
            result_field_name = op_details.result_field_name
            self._kiara.data_registry.add_destiny(
                category="metadata",
                key=metadata_key,
                values={input_field_name: value},
                manifest=op,
                result_field_name=result_field_name,
            )

    def resolve_all_metadata(self, value: Value):

        self._kiara.data_registry.resolve_destinies_for_value(
            value=value, category="metadata"
        )


class DataRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._engine: Engine = self._kiara._engine

        self._data_stores: Dict[uuid.UUID, DataStore] = {}
        self._default_data_store: Optional[uuid.UUID] = None
        self.add_store(FilesystemDataStore(kiara=self._kiara))

        self._registered_values: Dict[uuid.UUID, Value] = {}
        self._registred_jobs: Dict[int, JobRecord] = {}

        self._value_store_map: Dict[uuid.UUID, uuid.UUID] = {}
        # self._job_store_map: Dict[int, uuid.UUID] = {}

        self._destinies: Dict[uuid.UUID, Destiny] = {}
        self._destinies_by_value: Dict[
            uuid.UUID, Dict[str, Dict[str, Set[Destiny]]]
        ] = {}
        self._values_by_hash: Dict[int, Set[uuid.UUID]] = {}

        self._cached_data: Dict[uuid.UUID, Any] = {}
        self._load_configs: Dict[uuid.UUID, Optional[LoadConfig]] = {}

        self._alias_registry: AliasRegistry = None

        # initialize special values
        # special_value_cls = PythonClass.from_class(SpecialValue)
        # self._not_set_value: Value = Value(value_id=NOT_SET_VALUE_ID, kiara_id=self._kiara.id, value_schema=ValueSchema(type="special_type", default=SpecialValue.NOT_SET, is_constant=True, doc="Special value, indicating a field is not set."), value_status=ValueStatus.NOT_SET, value_size=0, value_hash=-1, pedigree=ORPHAN, pedigree_output_name="__void__", python_class=special_value_cls)
        # self._cached_data[NOT_SET_VALUE_ID] = SpecialValue.NOT_SET
        # self._none_value: Value = Value(value_id=NONE_VALUE_ID, kiara_id=self._kiara.id, value_schema=ValueSchema(type="special_type", default=SpecialValue.NO_VALUE, is_constant=True, doc="Special value, indicating a field is set with a 'none' value."), value_status=ValueStatus.NONE, value_size=0, value_hash=-2, pedigree=ORPHAN, pedigree_output_name="__void__", python_class=special_value_cls)
        # self._cached_data[NONE_VALUE_ID] = SpecialValue.NO_VALUE

        self._event_hooks: Dict[str, List[DataEventHook]] = {}

        self.add_hook(CreateMetadataDestinies(kiara=self._kiara))

    @property
    def aliases(self) -> AliasRegistry:

        if self._alias_registry is not None:
            return self._alias_registry

        root_doc = "The root for all value aliases."
        self._alias_registry = AliasRegistry()
        self._alias_registry._data_registry = self
        self._alias_registry._engine = self._engine

        return self._alias_registry

    def register_alias(self, alias: str, value: Union[Value, uuid.UUID]):

        value = self.get_value(value=value)
        self.aliases.set_alias(alias=alias, value_id=value.value_id)
        self.aliases.save(alias)

    def add_store(self, data_store: DataStore):
        if data_store.data_store_id in self._data_stores.keys():
            raise Exception(
                f"Can't add store, store id '{data_store.data_store_id}' already registered."
            )
        self._data_stores[data_store.data_store_id] = data_store
        if self._default_data_store is None:
            self._default_data_store = data_store.data_store_id

    @property
    def default_data_store(self) -> DataStore:
        if self._default_data_store is None:
            raise Exception("No default data store set.")
        return self._data_stores[self._default_data_store]

    @property
    def data_stores(self) -> Mapping[uuid.UUID, DataStore]:
        return self._data_stores

    def get_store(self, store_id: Optional[uuid.UUID] = None) -> DataStore:
        if store_id is None:
            return self.default_data_store
        else:
            return self._data_stores[store_id]

    def add_hook(self, hook: DataEventHook):

        event_types = hook.get_subscribed_event_types()
        if isinstance(event_types, str):
            event_types = [event_types]
        for event_type in event_types:
            self._event_hooks.setdefault(event_type, []).append(hook)

    def find_store_id_for_value(self, value: Union[uuid.UUID]) -> Optional[uuid.UUID]:

        if isinstance(value, Value):
            value = value.value_id

        if value in self._value_store_map.keys():
            return self._value_store_map[value]

        matches = []
        for store_id, store in self.data_stores.items():
            match = store.has_value(value_id=value)
            if match:
                matches.append(store_id)

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Found value with id '{value}' in multiple archives, this is not supported (yet): {matches}"
            )

        self._value_store_map[value] = matches[0]
        return matches[0]

    def get_value(self, value: Union[uuid.UUID, str, Value]) -> Value:

        if isinstance(value, str):
            value_id = uuid.UUID(value)
        elif isinstance(value, Value):
            value_id = value.value_id
            # TODO: check hash is the same as registered one?
        else:
            value_id = value

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
        stored_value = self.get_store(matches[0]).retrieve_value(value_id=value_id)
        stored_value._set_registry(self)
        self._registered_values[value_id] = stored_value
        return self._registered_values[value_id]

    def store_value(
        self,
        value: Value,
        aliases: Optional[Iterable[str]] = None,
        store_id: Optional[uuid.UUID] = None,
        skip_if_exists: bool = True,
    ):

        store = self.get_store(store_id=store_id)

        if not store.has_value(value.value_id) or not skip_if_exists:
            event = ValuePreStoreEvent.construct(kiara_id=self._kiara.id, value=value)
            self.send_event(event)
            load_config = store.store_value(value)
            self._value_store_map[value.value_id] = store.data_store_id
            self._load_configs[value.value_id] = load_config

            for category, keys in self.get_destinies_for_value(value=value).items():
                for key, destinies in keys.items():
                    for destiny in destinies:
                        if destiny.result_value_id is not None:
                            self.store_value(self.get_value(destiny.result_value_id))
                    store.persist_destinies(
                        value=value, category=category, key=key, destinies=destinies
                    )

        if aliases:
            aps_event = AliasPreStoreEvent.construct(
                kiara_id=self._kiara.id, value=value, aliases=aliases
            )
            self.send_event(aps_event)
            for alias in aliases:
                if not alias:
                    logger.debug("ignore.store.alias", reason="alias is empty")
                    continue
                self.register_alias(alias=alias, value=value)
            vs_event = ValueStoredEvent.construct(kiara_id=self._kiara.id, value=value)
            self.send_event(vs_event)

    def find_values_for_hash(
        self, value_hash: int, data_type_name: Optional[str] = None
    ) -> Set[Value]:

        if data_type_name:
            raise NotImplementedError()

        stored = self._values_by_hash.get(value_hash, None)
        if stored is None:
            matches: Dict[uuid.UUID, List[uuid.UUID]] = {}
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

        return set((self.get_value(value=v_id) for v_id in stored))

    def find_job_record(self, job: JobConfig) -> Optional[JobRecord]:

        if job.model_data_hash in self._registred_jobs.keys():
            return self._registred_jobs[job.model_data_hash]

        matches = []
        match = None
        for store_id, store in self.data_stores.items():
            match = store.retrieve_job_record(job=job)
            if match:
                matches.append(match)

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Multiple stores have a record for job '{job}', this is not supported (yet)."
            )

        # self._job_store_map[job.model_data_hash] = matches[0]
        self._registred_jobs[job.model_data_hash] = matches[0]

        return match

    def register_data(
        self,
        data: Any,
        schema: Optional[ValueSchema] = None,
        pedigree: Optional[ValuePedigree] = None,
        pedigree_output_name: str = None,
        reuse_existing: bool = True,
    ) -> Value:

        if schema is None:
            raise NotImplementedError()

        if pedigree is None:
            raise NotImplementedError()

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
            data = self.get_value(value=data)

        if isinstance(data, Value):
            if data.value_id in self._registered_values.keys():
                raise Exception(
                    f"Can't register value '{data.value_id}: already registered"
                )

            raise NotImplementedError()
            self._registered_values[data.value_id] = data
            return data

        data_type = self._kiara.get_data_type(
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

        v_id = uuid.uuid4()
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

        value._data_registry = self
        self._values_by_hash.setdefault(value.value_hash, set()).add(value.value_id)
        self._registered_values[value.value_id] = value
        self._cached_data[value.value_id] = data

        event = ValueCreatedEvent(kiara_id=self._kiara.id, value=value)
        self.send_event(event)
        return value

    def send_event(self, event: RegistryEvent, **payload):

        for hook in self._event_hooks.get(event.event_type, []):  # type: ignore
            hook.process_hook(event=event)

    def retrieve_load_config(self, value_id: uuid.UUID) -> Optional[LoadConfig]:

        if (
            value_id in self._load_configs.keys()
            and self._load_configs[value_id] is not None
        ):
            load_config = self._load_configs[value_id]
        else:
            # now, the value_store map should contain this value_id
            store_id = self.find_store_id_for_value(value=value_id)
            if store_id is None:
                return None

            store = self.get_store(store_id)
            self.get_value(value=value_id)
            load_config = store.retrieve_load_config(value=value_id)
            self._load_configs[value_id] = load_config

        return load_config

    def retrieve_value_data(self, value_id: uuid.UUID):

        if value_id in self._cached_data.keys():
            return self._cached_data[value_id]

        value = self.get_value(value=value_id)

        load_config = self.retrieve_load_config(value_id=value_id)

        if load_config is None:
            raise Exception(
                f"Load config for value '{value_id}' is 'None', this is most likely a bug."
            )

        data = self._load_data_from_load_config(load_config=load_config, value=value)
        self._cached_data[value_id] = data
        return data

    def _load_data_from_load_config(self, load_config: LoadConfig, value: Value) -> Any:

        logger.debug("value.load", module=load_config.module_type)

        # TODO: check whether modules and value types are available

        try:
            job_config = self._kiara.jobs_mgmt.prepare_job_config(
                manifest=load_config, inputs=load_config.inputs
            )
        except JobConfigException as jce:
            if is_debug():
                import traceback

                traceback.print_exc()
            return UnloadableData(value=value, load_config=load_config)

        result = self._kiara.jobs_mgmt.execute_job(job_config=job_config)

        # data = result.get_value_data(load_config.output_name)
        result_value = result.get_value_obj(field_name=load_config.output_name)
        return result_value.data

    def load_valueset(self, values: Mapping[str, uuid.UUID]):

        value_items = {}
        schemas = {}
        for field_name, value_id in values.items():
            value = self.get_value(value=value_id)
            value_items[field_name] = value
            schemas[field_name] = value.value_schema

        return ValueSetReadOnly(value_items=value_items, values_schema=schemas)

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
                    f"Can't register job, inputs contain unused/invalid fields: {', '.join(leftover)}"
                )

        values = {}

        for input_name, details in input_details.items():

            value_schema = details["schema"]

            if input_name not in data.keys():
                value_data = SpecialValue.NOT_SET
            elif data[input_name] == None:
                value_data = SpecialValue.NO_VALUE
            else:
                value_data = data[input_name]

            value = self.retrieve_or_create_value(value_data, value_schema=value_schema)
            values[input_name] = value

        return ValueSetReadOnly(value_items=values, values_schema=schema)  # type: ignore

    def retrieve_or_create_value(
        self, value_or_data: Any, value_schema: ValueSchema
    ) -> Value:

        if isinstance(value_or_data, Value):
            existing = self.get_value(value_or_data)
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

            # TODO: check for aliases later
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

    def add_destiny(
        self,
        category: str,
        key: str,
        values: Dict[str, Union[Value, uuid.UUID]],
        manifest: Manifest,
        result_field_name: Optional[str] = None,
    ) -> Destiny:

        if not values:
            raise Exception("Can't add destiny, no values provided.")

        value_ids: Dict[str, uuid.UUID] = {
            k: (value if isinstance(value, uuid.UUID) else value.value_id)
            for k, value in values.items()
        }

        duplicates = []
        for value_id in value_ids.values():
            if (
                self._destinies_by_value.get(value_id, {})
                .get(category, {})
                .get(key, None)
                is not None
            ):
                duplicates.append(value_id)

        if duplicates:
            for value_id in duplicates:
                log_message(
                    "duplicate.destiny", category=category, key=key, value_id=value_id
                )
            # raise Exception(f"Can't add destiny, already existing destiny for value(s): {', '.join(duplicates)}.")

        destiny = Destiny.create_from_values(
            kiara=self._kiara,
            category=category,
            key=key,
            manifest=manifest,
            result_field_name=result_field_name,
            values=value_ids,
        )

        for value_id in value_ids.values():
            self._destinies[destiny.destiny_id] = destiny
            self._destinies_by_value.setdefault(value_id, {}).setdefault(
                category, {}
            ).setdefault(key, set()).add(destiny)

        return destiny

    def get_destinies_for_value(
        self,
        value: Union[uuid.UUID, Value],
        category: Optional[str] = None,
        key: Optional[str] = None,
    ) -> Mapping[str, Mapping[str, Set[Destiny]]]:

        if isinstance(value, Value):
            value = value.value_id

        destinies = self._destinies_by_value.get(value, {})
        if category is not None:
            temp = {}
            temp[category] = destinies.get(category, {})
            destinies = temp

        if key is not None:
            temp = {}
            for category, keys in destinies.items():
                for _key, _destinies in keys.items():
                    if key == _key:
                        temp.setdefault(category, {})[key] = _destinies

            destinies = temp
        return destinies

    def resolve_destinies_for_value(
        self,
        value: Union[uuid.UUID, Value],
        category: Optional[str] = None,
        key: Optional[str] = None,
    ) -> Dict[str, Dict[str, Dict[uuid.UUID, Value]]]:

        result = {}
        destinies = self.get_destinies_for_value(
            value=value, category=category, key=key
        )
        for category, keys in destinies.items():
            for key, destinies in keys.items():
                for destiny in destinies:
                    value = self.resolve_destiny(destiny)
                    result.setdefault(category, {}).setdefault(key, {})[
                        destiny.destiny_id
                    ] = value

        return result

    def resolve_destiny(self, destiny: Destiny) -> Value:

        result = self._kiara.execute(manifest=destiny, inputs=destiny.merged_inputs)
        value = result.get_value_obj(field_name=destiny.result_field_name)
        destiny.result_value_id = value.value_id
        return value

    def render_data(
        self, value: Value, target_type="terminal_renderable", **render_config: Any
    ) -> Any:

        if render_config:
            raise NotImplementedError()

        op_type: RenderValueOperationType = self._kiara.operations_mgmt.get_operation_type("render_value")  # type: ignore
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

        result = op.run(kiara=self._kiara, inputs={"value": value})
        rendered = result.get_value_data("rendered_value")
        return rendered
