# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc
import copy
import uuid
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Union,
)

import structlog
from rich.console import RenderableType

from kiara.data_types import DataType
from kiara.data_types.included_core_types import NoneType
from kiara.defaults import (
    DATA_ARCHIVE_DEFAULT_VALUE_MARKER,
    DEFAULT_DATA_STORE_MARKER,
    DEFAULT_STORE_MARKER,
    ENVIRONMENT_MARKER_KEY,
    INVALID_HASH_MARKER,
    NO_SERIALIZATION_MARKER,
    NONE_STORE_ID,
    NONE_VALUE_ID,
    NOT_SET_VALUE_ID,
    ORPHAN_PEDIGREE_OUTPUT_NAME,
    STRICT_CHECKS,
    SpecialValue,
)
from kiara.exceptions import (
    InvalidValuesException,
    KiaraException,
    NoSuchValueAliasException,
    NoSuchValueException,
    NoSuchValueIdException,
)
from kiara.interfaces.python_api.models.info import ValueInfo
from kiara.models.events.data_registry import (
    DataArchiveAddedEvent,
    ValueCreatedEvent,
    ValuePreStoreEvent,
    ValueRegisteredEvent,
    ValueStoredEvent,
)
from kiara.models.module.operation import Operation
from kiara.models.python_class import PythonClass
from kiara.models.values import DEFAULT_SCALAR_DATATYPE_CHARACTERISTICS, ValueStatus
from kiara.models.values.matchers import ValueMatcher
from kiara.models.values.value import (
    ORPHAN,
    DataTypeInfo,
    PersistedData,
    SerializationMetadata,
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
from kiara.utils.data import pretty_print_data
from kiara.utils.hashing import NONE_CID
from kiara.utils.stores import check_external_archive

if TYPE_CHECKING:
    from multiformats.varint import BytesLike

    from kiara.context import Kiara
    from kiara.models.module.destiny import Destiny
    from kiara.models.module.manifest import Manifest


logger = structlog.getLogger()


class ValueLink(Protocol):

    value_id: uuid.UUID


NONE_PERSISTED_DATA = PersistedData(
    data_type="none",
    data_type_config={},
    serialization_profile="none",
    metadata=SerializationMetadata(),
    hash_codec="sha2-256",
    archive_id=NONE_STORE_ID,
    chunk_id_map={},
)


class AliasResolver(abc.ABC):
    def __init__(self, kiara: "Kiara"):

        self._kiara: "Kiara" = kiara

    @abc.abstractmethod
    def resolve_alias(self, alias: str) -> uuid.UUID:
        pass


ARCHIVE_REF_TYPE_NAME = "archive"


class DefaultAliasResolver(AliasResolver):
    def __init__(self, kiara: "Kiara"):

        super().__init__(kiara=kiara)

    def resolve_alias(self, alias: str) -> uuid.UUID:

        # preprocessing alias
        if alias.endswith(".kiarchive"):
            alias = f"archive:{alias}"

        if ":" in alias:
            ref_type, rest = alias.split(":", maxsplit=1)

            if ref_type == "value":
                _value_id: Union[uuid.UUID, None] = uuid.UUID(rest)
            elif ref_type == "alias":
                _value_id = self._kiara.alias_registry.find_value_id_for_alias(
                    alias=rest
                )
                if _value_id is None:
                    raise NoSuchValueAliasException(
                        alias=rest,
                        msg=f"Can't retrive value for alias '{rest}': no such alias registered.",
                    )
            elif ref_type == ARCHIVE_REF_TYPE_NAME:

                if "#" in rest:
                    archive_ref, path_in_archive = rest.split("#", maxsplit=1)
                else:
                    archive_ref = rest
                    path_in_archive = None

                archives = check_external_archive(
                    archive=archive_ref, allow_write_access=False
                )

                if archives:
                    data_archive: DataArchive = archives.get("data", None)  # type: ignore
                    if data_archive:
                        self._kiara.data_registry.register_data_archive(data_archive)

                        if not path_in_archive:
                            default_value = data_archive.get_archive_metadata(
                                DATA_ARCHIVE_DEFAULT_VALUE_MARKER
                            )
                            if default_value is None:
                                raise NoSuchValueException(
                                    f"No default value found for uri: {alias}"
                                )
                            _value_id = uuid.UUID(default_value)
                        else:
                            from kiara.registries.aliases import AliasArchive

                            alias_archive: AliasArchive = archives.get("alias", None)  # type: ignore
                            if alias_archive:
                                _value_id = alias_archive.find_value_id_for_alias(
                                    alias=path_in_archive
                                )
                            else:
                                raise NoSuchValueException(
                                    msg=f"No alias archive found for '{archive_ref}'."
                                )
                    else:
                        raise NoSuchValueException(
                            "No data archive found in '{archive_ref}'."
                        )
                else:
                    raise NoSuchValueException(
                        msg=f"No archive found for '{archive_ref}'."
                    )

            else:
                raise Exception(
                    f"Can't retrieve value for '{alias}': invalid reference type '{ref_type}'."
                )
        else:
            _value_id = self._kiara.alias_registry.find_value_id_for_alias(alias)
            if _value_id is None:
                raise Exception(
                    f"Can't retrieve value for alias '{alias}': no such alias registered."
                )

        if _value_id is None:
            raise Exception(
                f"Can't retrieve value for alias '{alias}': no such alias registered."
            )
        return _value_id


class DataRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

        self._event_callback: Callable = self._kiara.event_registry.add_producer(self)

        self._data_archives: Dict[str, DataArchive] = {}

        self._default_data_store: Union[str, None] = None
        self._registered_values: Dict[uuid.UUID, Value] = {}

        self._value_archive_lookup_map: Dict[uuid.UUID, str] = {}
        """A cached dict that stores which archives which value ids belong to."""

        self._values_by_hash: Dict[str, Set[uuid.UUID]] = {}

        self._cached_data: Dict[uuid.UUID, Any] = {}
        self._persisted_value_descs: Dict[uuid.UUID, Union[PersistedData, None]] = {}

        self._alias_resolver: AliasResolver = DefaultAliasResolver(kiara=self._kiara)

        # initialize special values
        special_value_cls = PythonClass.from_class(NoneType)
        data_type_info = DataTypeInfo(
            data_type_name="none",
            characteristics=DEFAULT_SCALAR_DATATYPE_CHARACTERISTICS,
            data_type_class=special_value_cls,
        )
        self._not_set_value: Value = Value(
            value_id=NOT_SET_VALUE_ID,
            kiara_id=self._kiara.id,
            value_schema=ValueSchema(
                type="none",
                default=SpecialValue.NOT_SET,
                is_constant=True,
                doc="Special value, indicating a field is not set.",  # type: ignore
            ),
            environment_hashes={},
            value_status=ValueStatus.NOT_SET,
            value_size=0,
            value_hash=INVALID_HASH_MARKER,
            pedigree=ORPHAN,
            pedigree_output_name="__void__",
            data_type_info=data_type_info,
        )
        self._not_set_value._data_registry = self
        self._cached_data[NOT_SET_VALUE_ID] = SpecialValue.NOT_SET
        self._registered_values[NOT_SET_VALUE_ID] = self._not_set_value
        self._persisted_value_descs[NOT_SET_VALUE_ID] = NONE_PERSISTED_DATA
        # self._env_cache: Dict[str, Dict[str, RuntimeEnvironment]] = {}

        self._none_value: Value = Value(
            value_id=NONE_VALUE_ID,
            kiara_id=self._kiara.id,
            value_schema=ValueSchema(
                type="none",
                default=SpecialValue.NO_VALUE,
                is_constant=True,
                doc="Special value, indicating a field is set with a 'none' value.",  # type: ignore
            ),
            environment_hashes={},
            value_status=ValueStatus.NONE,
            value_size=0,
            value_hash=str(NONE_CID),
            pedigree=ORPHAN,
            pedigree_output_name="__void__",
            data_type_info=data_type_info,
        )
        self._none_value._data_registry = self
        self._cached_data[NONE_VALUE_ID] = SpecialValue.NO_VALUE
        self._registered_values[NONE_VALUE_ID] = self._none_value
        self._persisted_value_descs[NONE_VALUE_ID] = NONE_PERSISTED_DATA

        self._cached_value_aliases: Dict[uuid.UUID, Dict[str, Union[Destiny, None]]] = (
            {}
        )

        self._destinies: Dict[uuid.UUID, Destiny] = {}
        self._destinies_by_value: Dict[uuid.UUID, Dict[str, Destiny]] = {}

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
        for alias, store in self._data_archives.items():
            ids = store.value_ids
            if ids:
                result.update(ids)

        return result

    def register_data_archive(
        self,
        archive: DataArchive,
        set_as_default_store: Union[bool, None] = None,
    ) -> str:

        alias = archive.archive_name

        if not alias:
            raise Exception("Invalid data archive alias: can't be empty.")

        if alias in self._data_archives.keys():
            raise Exception(
                f"Can't add data archive, alias '{alias}' already registered."
            )

        archive.register_archive(kiara=self._kiara)

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

        event = DataArchiveAddedEvent(
            kiara_id=self._kiara.id,
            data_archive_id=archive.archive_id,
            data_archive_alias=alias,
            is_store=is_store,
            is_default_store=is_default_store,
        )
        self._event_callback(event)

        return alias

    @property
    def default_data_store(self) -> str:
        if self._default_data_store is None:
            raise Exception("No default data store set.")
        return self._default_data_store

    @property
    def data_archives(self) -> Mapping[str, DataArchive]:
        return self._data_archives

    def get_archive(
        self, archive_id_or_alias: Union[None, uuid.UUID, str] = None
    ) -> DataArchive:

        if archive_id_or_alias in (
            None,
            DEFAULT_STORE_MARKER,
            DEFAULT_DATA_STORE_MARKER,
        ):
            archive_id_or_alias = self.default_data_store
            if archive_id_or_alias is None:
                raise Exception("Can't retrieve default data archive, none set (yet).")

        if isinstance(archive_id_or_alias, uuid.UUID):
            for archive in self._data_archives.values():
                if archive.archive_id == archive_id_or_alias:
                    return archive

            raise Exception(
                f"Can't retrieve archive with id '{archive_id_or_alias}': no archive with that id registered."
            )

        if archive_id_or_alias in self._data_archives.keys():
            return self._data_archives[archive_id_or_alias]
        else:
            try:
                _archive_id = uuid.UUID(archive_id_or_alias)
                for archive in self._data_archives.values():
                    if archive.archive_id == _archive_id:
                        return archive
                    raise Exception(
                        f"Can't retrieve archive with id '{archive_id_or_alias}': no archive with that id registered."
                    )
            except Exception:
                pass

        raise Exception(
            f"Can't retrieve archive with id '{archive_id_or_alias}': no archive with that id registered."
        )

    def find_store_id_for_value(self, value_id: uuid.UUID) -> Union[str, None]:

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

    def get_value(self, value: Union[uuid.UUID, ValueLink, str, Path]) -> Value:
        _value_id = None

        if not isinstance(value, uuid.UUID):
            # fallbacks for common mistakes, this should error out if not a Value or string.
            if hasattr(value, "value_id"):
                _value_id: Union[uuid.UUID, str, None] = value.value_id  # type: ignore
                if isinstance(_value_id, str):
                    _value_id = uuid.UUID(_value_id)
            else:

                try:
                    _value_id = uuid.UUID(
                        value  # type: ignore
                    )  # this should fail if not string or wrong string format
                except ValueError:
                    _value_id = None

                if _value_id is None:
                    if isinstance(value, Path):
                        raise NotImplementedError()
                    if not isinstance(value, str):
                        raise Exception(
                            f"Can't retrieve value for '{value}': invalid type '{type(value)}'."
                        )
                    try:
                        _value_id = self._alias_resolver.resolve_alias(value)
                    except Exception as e:
                        log_exception(e)
                        raise e
        else:
            _value_id = value

        assert _value_id is not None

        if _value_id in self._registered_values.keys():
            _value = self._registered_values[_value_id]
            return _value

        default_store: DataArchive = self.get_archive(
            archive_id_or_alias=self.default_data_store
        )
        if not default_store.has_value(value_id=_value_id):

            matches = []
            for store_id, store in self.data_archives.items():
                match = store.has_value(value_id=_value_id)
                if match:
                    matches.append(store_id)

            if len(matches) == 0:
                raise NoSuchValueIdException(
                    value_id=_value_id, msg=f"No value registered with id: {value}"
                )
            elif len(matches) > 1:
                raise NoSuchValueIdException(
                    value_id=_value_id,
                    msg=f"Found value with id '{value}' in multiple archives, this is not supported (yet): {matches}",
                )
            store_that_has_it = matches[0]
        else:
            store_that_has_it = self.default_data_store

        self._value_archive_lookup_map[_value_id] = store_that_has_it

        stored_value = self.get_archive(store_that_has_it).retrieve_value(
            value_id=_value_id
        )
        stored_value._set_registry(self)
        stored_value._is_stored = True

        self._registered_values[_value_id] = stored_value
        return self._registered_values[_value_id]

    def _persist_environment(self, env_hash: str, store: Union[str, None]):

        # cached = self._env_cache.get(env_type, {}).get(env_hash, None)
        # if cached is not None:
        #     return

        environment = self._kiara.metadata_registry.retrieve_environment_item(env_hash)

        if not environment:
            raise KiaraException(
                f"Can't persist data environment with hash '{env_hash}': no such environment registered."
            )

        self._kiara.metadata_registry.register_metadata_item(
            key=ENVIRONMENT_MARKER_KEY, item=environment, store=store
        )
        # self._env_cache.setdefault(env_type, {})[env_hash] = environment

    def store_value(
        self,
        value: Union[ValueLink, uuid.UUID, str],
        data_store: Union[str, None] = None,
    ) -> Union[PersistedData, None]:
        """Store a value into a data store.

        If 'data_store' is not provided, the default data store is used. If the 'data_store' argument is of
        type uuid, the archive_id is used, if string, first it will be converted to an uuid, if that works,
        again, the archive_id is used, if not, the string is used as the archive alias.

        """

        _value = self.get_value(value)

        # first, persist environment information
        for env_hash in _value.pedigree.environments.values():

            self._persist_environment(env_hash, store=data_store)

        store: DataStore = self.get_archive(archive_id_or_alias=data_store)  # type: ignore
        if not store.is_writeable():
            if data_store:
                raise Exception(
                    f"Can't write value into store '{data_store}': not writable."
                )
            else:
                raise Exception("Can't write value into store: not writable.")

        _data_store = store.archive_name
        # make sure all property values are available
        if _value.pedigree != ORPHAN:
            for value_id in _value.pedigree.inputs.values():
                self.store_value(value=value_id, data_store=_data_store)

        if not store.has_value(_value.value_id):
            event = ValuePreStoreEvent(kiara_id=self._kiara.id, value=_value)
            self._event_callback(event)
            persisted_value = store.store_value(_value)
            _value._is_stored = True

            self._value_archive_lookup_map[_value.value_id] = _data_store
            self._persisted_value_descs[_value.value_id] = persisted_value
            property_values = _value.property_values

            for property, property_value in property_values.items():
                self.store_value(value=property_value, data_store=_data_store)

            store_required = True
        else:
            persisted_value = None
            store_required = False

        store_event = ValueStoredEvent(
            kiara_id=self._kiara.id, value=_value, storing_required=store_required
        )
        self._event_callback(store_event)

        if _value.job_id:
            self._kiara.job_registry.store_job_record(
                job_id=_value.job_id, store=data_store
            )

        return persisted_value

    def lookup_aliases(self, value: Union[Value, uuid.UUID]) -> Set[str]:

        if isinstance(value, Value):
            value = value.value_id

        return self._kiara.alias_registry.find_aliases_for_value_id(value_id=value)

    def create_value_info(self, value: Union[Value, uuid.UUID]) -> ValueInfo:

        if isinstance(value, uuid.UUID):
            value = self.get_value(value=value)

        value_info = ValueInfo.create_from_instance(kiara=self._kiara, instance=value)
        return value_info

    def find_values(self, matcher: ValueMatcher) -> Dict[uuid.UUID, Value]:

        matches: Dict[uuid.UUID, Value] = {}
        for store_id, store in self.data_archives.items():

            if matcher.in_data_archives and store_id not in matcher.in_data_archives:
                continue

            try:
                _matches = store.find_values(matcher=matcher)
                for value in _matches:
                    if value.value_id in matches.keys():
                        raise Exception(
                            f"Found value '{value.value_id}' multiple times, this is not supported yet."
                        )
                    self._value_archive_lookup_map[value.value_id] = store_id
                    value._set_registry(self)
                    value._is_stored = True
                    self._registered_values[value.value_id] = value
                    matches[value.value_id] = value
                    self._values_by_hash.setdefault(value.value_hash, set()).add(
                        value.value_id
                    )
            except NotImplementedError:
                log_message(
                    "store.feature.missing",
                    feature="find_value",
                    reasong=f"find_values not implemented for store: {store_id}",
                    solution="return all values",
                )

                all_value_ids = store.value_ids
                if all_value_ids is None:
                    continue
                for value_id in all_value_ids:
                    value = store.retrieve_value(value_id=value_id)
                    value._set_registry(self)
                    value._is_stored = True

                    self._registered_values[value.value_id] = value

                    match = matcher.is_match(value, kiara=self._kiara)
                    if match:
                        if value.value_id in matches.keys():
                            raise Exception(
                                f"Found value '{value.value_id}' multiple times, this is not supported yet."
                            )
                        matches[value.value_id] = value

        return matches

    def find_values_with_aliases(self, matcher: ValueMatcher) -> Dict[str, Value]:

        matcher = matcher.model_copy(update={"has_aliases": True})
        all_values = self.find_values(matcher)
        result = {}
        for value in all_values.values():
            aliases = self._kiara.alias_registry.find_aliases_for_value_id(
                value_id=value.value_id
            )
            for a in aliases:
                assert a not in result  # this is a bug
                result[a] = value

        return result

    def find_values_for_hash(
        self, value_hash: str, data_type_name: Union[str, None] = None
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

        return {self.get_value(value=v_id) for v_id in stored}

    # ==============================================================================================
    # destiny stuff

    def retrieve_destinies_for_value_from_archives(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Mapping[str, uuid.UUID]:

        if alias_filter:
            raise NotImplementedError()

        all_destinies: Dict[str, uuid.UUID] = {}
        for archive_id, archive in self._data_archives.items():
            destinies: Union[Mapping[str, uuid.UUID], None] = (
                archive.find_destinies_for_value(
                    value_id=value_id, alias_filter=alias_filter
                )
            )
            if not destinies:
                continue
            for k, v in destinies.items():
                if k in all_destinies.keys():
                    raise Exception(f"Duplicate destiny '{k}' for value '{value_id}'.")
                all_destinies[k] = v

        return all_destinies

    def get_destiny_aliases_for_value(
        self, value_id: uuid.UUID, alias_filter: Union[str, None] = None
    ) -> Iterable[str]:

        # TODO: cache the result of this

        if alias_filter is not None:
            raise NotImplementedError()

        aliases: Set[str] = set()
        aliases.update(
            self.retrieve_destinies_for_value_from_archives(value_id=value_id).keys()
        )

        # all_stores = self._all_values_store_map.get(value_id)
        # if all_stores:
        #     for prefix in all_stores:
        #         all_aliases = self._destiny_archives[
        #             prefix
        #         ].get_destiny_aliases_for_value(value_id=value_id)
        #         if all_aliases is not None:
        #             aliases.update((f"{prefix}.{a}" for a in all_aliases))

        current = self._destinies_by_value.get(value_id, None)
        if current:
            aliases.update(current.keys())

        return sorted(aliases)

    def register_destiny(
        self,
        destiny_alias: str,
        values: Dict[str, uuid.UUID],
        manifest: "Manifest",
        result_field_name: Union[str, None] = None,
    ) -> "Destiny":
        """
        Add a destiny for one (or in some rare cases several) values.

        A destiny alias must be unique for every one of the involved input values.
        """
        if not values:
            raise Exception("Can't add destiny, no values provided.")

        from kiara.models.module.destiny import Destiny

        destiny = Destiny.create_from_values(
            kiara=self._kiara,
            destiny_alias=destiny_alias,
            manifest=manifest,
            result_field_name=result_field_name,
            values=values,
        )

        for value_id in destiny.fixed_inputs.values():

            self._destinies[destiny.destiny_id] = destiny
            # TODO: store history?
            self._destinies_by_value.setdefault(value_id, {})[destiny_alias] = destiny
            self._cached_value_aliases.setdefault(value_id, {})[destiny_alias] = destiny

        return destiny

    def attach_destiny_as_property(
        self,
        destiny: Union[uuid.UUID, "Destiny"],
        field_names: Union[Iterable[str], None] = None,
    ):

        if field_names:
            raise NotImplementedError()

        if isinstance(destiny, uuid.UUID):
            destiny = self._destinies[destiny]

        values = self.load_values(destiny.fixed_inputs)

        already_stored: List[uuid.UUID] = []
        for v in values.values():
            if v.is_stored:
                already_stored.append(v.value_id)

        if already_stored:
            stored = (str(v) for v in already_stored)
            raise Exception(
                f"Can't attach destiny as property, value(s) already stored: {', '.join(stored)}"
            )

        if destiny.result_value_id is None:
            destiny.execute(kiara=self._kiara)

        for v in values.values():
            assert destiny.result_value_id is not None
            v.add_property(
                value_id=destiny.result_value_id,
                property_path=destiny.destiny_alias,
                add_origin_to_property_value=True,
            )

    def get_registered_destiny(
        self, value_id: uuid.UUID, destiny_alias: str
    ) -> "Destiny":

        destiny = self._destinies_by_value.get(value_id, {}).get(destiny_alias, None)
        if destiny is None:
            raise Exception(
                f"No destiny '{destiny_alias}' available for value '{value_id}'."
            )

        return destiny

    def register_data(
        self,
        data: Any,
        schema: Union[ValueSchema, str, None, Mapping[str, Any]] = None,
        pedigree: Union[ValuePedigree, None] = None,
        pedigree_output_name: Union[str, None] = None,
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
        self, data: Any, schema: Union[ValueSchema, None]
    ) -> Tuple[
        Union[Value, None],
        DataType,
        Union[Any, None],
        Union[str, SerializedData],
        ValueStatus,
        str,
        int,
    ]:

        if schema is None:
            raise NotImplementedError()

        if isinstance(data, Value):

            if data.value_id in self._registered_values.keys():

                if data.is_set and data.is_serializable:
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
            value = self.get_value(value=data)
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
        except NoSuchValueException as nsve:
            raise nsve
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

        existing_value: Union[Value, None] = None
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
        schema: Union[None, str, ValueSchema, Mapping[str, Any]] = None,
        pedigree: Union[ValuePedigree, None] = None,
        pedigree_output_name: Union[str, None] = None,
        reuse_existing: bool = True,
    ) -> Tuple[Value, bool]:
        """
        Create a new value, or return an existing one that matches the incoming data or reference.

        Arguments:
        ---------
            data: the (raw) data, or a reference to an existing value


        Returns:
        -------
            a tuple containing of the value object, and a boolean indicating whether the value was newly created (True), or already existing (False)
        """

        if schema is None:
            raise NotImplementedError()
        elif isinstance(schema, str):
            schema = ValueSchema(type=schema)
        elif isinstance(schema, Mapping):
            schema = ValueSchema(**schema)
        elif not isinstance(schema, ValueSchema):
            raise Exception(
                f"Invalid schema type: {type(schema)}, expected: {ValueSchema}"
            )

        if schema.type not in self._kiara.data_type_names:
            raise Exception(
                f"Can't register data of type '{schema.type}': type not registered. Available types: {', '.join(self._kiara.data_type_names)}"
            )

        if data is SpecialValue.NOT_SET and schema.default is not SpecialValue.NOT_SET:
            if callable(schema.default):
                raise NotImplementedError()
                data = schema.default()
            else:
                data = copy.deepcopy(schema.default)

            reuse_existing = False

        data_type: Union[None, DataType] = None
        if reuse_existing and not isinstance(data, (Value, uuid.UUID, SpecialValue)):

            data_type = self._kiara.type_registry.retrieve_data_type(
                data_type_name=schema.type, data_type_config=schema.type_config
            )
            if data_type.characteristics.is_scalar:
                reuse_existing = False

        if data is None:
            data = SpecialValue.NO_VALUE
        elif isinstance(data, uuid.UUID):
            if data == NONE_VALUE_ID:
                data = SpecialValue.NO_VALUE
            elif data == NOT_SET_VALUE_ID:
                data = SpecialValue.NOT_SET

        if reuse_existing and data not in [SpecialValue.NO_VALUE, SpecialValue.NOT_SET]:
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

            if data_type is None:
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

        if not pedigree.is_resolved:
            pedigree = self._kiara.module_registry.resolve_manifest(pedigree)  # type: ignore

        v_id = ID_REGISTRY.generate(
            type="value", kiara_id=self._kiara.id, pre_registered=False
        )

        value, data = data_type.assemble_value(
            value_id=v_id,
            data=data,
            schema=schema,
            environment_hashes=self._kiara.environment_registry.environment_hashes,
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
    ) -> Union[SerializedData, None]:
        """Create a LoadConfig object from the details of the persisted version of this value."""
        pv = self.retrieve_persisted_value_details(value_id=value_id)
        if pv is None:
            return None

        return pv

    # def retrieve_chunk(
    #     self,
    #     chunk_id: str,
    #     archive_id: Union[uuid.UUID, None] = None,
    #     as_file: bool = True,
    #     symlink_ok: bool = True,
    # ) -> Union[str, "BytesLike"]:
    #
    #     if archive_id is None:
    #         raise NotImplementedError()
    #
    #     archive = self.get_archive(archive_id)
    #     chunk = archive.retrieve_chunk(chunk_id, as_file=as_file, symlink_ok=symlink_ok)
    #
    #     return chunk

    def retrieve_chunks(
        self,
        chunk_ids: Sequence[str],
        as_files: bool = True,
        symlink_ok: bool = True,
        archive_id: Union[uuid.UUID, None] = None,
    ) -> Generator[Union[str, "BytesLike"], None, None]:
        """Return the chunk content in the same order as the 'chunk_ids' argument.

        If 'as_files' is 'True', it will return strings representing paths to files containing the chunk data. If symlink_ok is also set to 'True', the returning Path could potentially be a symlink, which means the underlying function might not need to copy the file. In this case, you are responsible to not change the contents of the path, ever.

        If 'as_files' is 'False', BytesLike objects will be returned, containing the chunk data bytes directly.
        """

        if archive_id is None:
            raise NotImplementedError(
                "Can't retrieve chunks without specifying an archive."
            )

        archive = self.get_archive(archive_id)

        chunks = archive.retrieve_chunks(
            chunk_ids, as_files=as_files, symlink_ok=symlink_ok
        )

        return chunks
        # for chunk_id in chunk_ids:
        #     yield archive.retrieve_chunk(chunk_id)

    def retrieve_value_data(
        self, value: Union[uuid.UUID, Value], target_profile: Union[str, None] = None
    ) -> Any:

        if isinstance(value, uuid.UUID):
            value = self.get_value(value=value)

        if value.value_id in self._cached_data.keys():
            return self._cached_data[value.value_id]

        if value._serialized_data is None:
            serialized_data: Union[str, SerializedData] = (
                self.retrieve_persisted_value_details(value_id=value.value_id)
            )
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

        module = self._kiara.module_registry.create_module(manifest=manifest)
        op = Operation.create_from_module(module=module)

        input_field_match: Union[str, None] = None

        if len(op.inputs_schema) == 1:
            input_field_match = next(iter(op.inputs_schema.keys()))
        else:
            for input_field, schema in op.inputs_schema.items():
                for dt in self._kiara.type_registry.get_type_lineage(
                    value.data_type_name
                ):
                    if schema.type == dt:
                        if input_field_match is not None:
                            raise Exception(
                                f"Can't determine input field for deserialization operation '{module.module_type_name}': multiple input fields with type '{input_field_match}'."
                            )
                        else:
                            input_field_match = input_field
                            break
                if input_field_match:
                    break

        if input_field_match is None:
            raise Exception(
                f"Can't determine input field for deserialization operation '{module.module_type_name}'."
            )

        result_field_match: Union[str, None] = None
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

        # TODO: can we do without this?
        parsed = value.data_type.parse_python_obj(python_object)
        value.data_type._validate(parsed)

        self._cached_data[value.value_id] = parsed

        return parsed

    def load_values(
        self,
        values: Mapping[str, Union[uuid.UUID, None, str, ValueLink]],
        values_schema: Union[None, Mapping[str, ValueSchema]] = None,
    ) -> ValueMapReadOnly:

        value_items = {}
        if values_schema:

            schemas = values_schema
            for k in schemas.keys():
                if k not in values.keys():
                    value_items[k] = self.get_value(NOT_SET_VALUE_ID)
                else:
                    value_id = values[k]
                    if value_id is None:
                        value_id = NONE_VALUE_ID

                    value_items[k] = self.get_value(value_id)
        else:
            schemas = {}
            for field_name, value_id in values.items():
                if value_id is None:
                    value_id = NONE_VALUE_ID

                value = self.get_value(value=value_id)
                value_items[field_name] = value
                schemas[field_name] = value.value_schema

        return ValueMapReadOnly(value_items=value_items, values_schema=schemas)

    def load_data(
        self, values: Mapping[str, Union[uuid.UUID, None]]
    ) -> Mapping[str, Any]:

        result_values = self.load_values(values=values)
        return {k: v.data for k, v in result_values.items()}

    def create_valuemap(
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

        _resolved: Dict[str, Value] = {}
        _unresolved = {}
        for input_name, details in input_details.items():
            _d = data.get(input_name, SpecialValue.NOT_SET)
            if isinstance(_d, str):
                try:
                    _d = uuid.UUID(_d)
                except Exception:
                    if schema[input_name].type == "string" and not (
                        _d.startswith("alias:") or _d.startswith("value:")
                    ):
                        pass
                    else:
                        try:
                            _d = self._alias_resolver.resolve_alias(_d)
                        except Exception as e:
                            log_exception(e)

            if isinstance(_d, Value):
                _resolved[input_name] = _d
            elif isinstance(_d, uuid.UUID):
                _resolved[input_name] = self.get_value(_d)
            else:
                _unresolved[input_name] = _d

        for input_name, _value in _resolved.items():
            # TODO: validate values against schema
            values[input_name] = _value

        for input_name, _data in _unresolved.items():

            value_schema = input_details[input_name]["schema"]

            if input_name not in data.keys():
                value_data = SpecialValue.NOT_SET
            elif data[input_name] in [
                None,
                SpecialValue.NO_VALUE,
                SpecialValue.NOT_SET,
            ]:
                value_data = SpecialValue.NO_VALUE
            else:
                value_data = data[input_name]

            try:
                value = self.register_data(
                    data=value_data, schema=value_schema, reuse_existing=True
                )
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

    def pretty_print_data(
        self,
        value_id: uuid.UUID,
        target_type="terminal_renderable",
        **render_config: Any,
    ) -> Any:

        assert isinstance(value_id, uuid.UUID)

        return pretty_print_data(
            kiara=self._kiara,
            value_id=value_id,
            target_type=target_type,
            **render_config,
        )
