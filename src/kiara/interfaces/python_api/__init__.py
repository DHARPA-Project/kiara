# -*- coding: utf-8 -*-
import inspect
import json

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import structlog
import textwrap
import uuid
from functools import cached_property
from pathlib import Path
from ruamel.yaml import YAML
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Type, Union

from kiara.exceptions import DataTypeUnknownException, NoSuchWorkflowException
from kiara.interfaces.python_api.models.info import (
    DataTypeClassesInfo,
    DataTypeClassInfo,
    ModuleTypeInfo,
    ModuleTypesInfo,
    OperationGroupInfo,
    OperationInfo,
    OperationTypeInfo,
    ValueInfo,
    ValuesInfo,
)
from kiara.interfaces.python_api.models.workflow import WorkflowMatcher
from kiara.interfaces.python_api.value import StoreValueResult, StoreValuesResult
from kiara.interfaces.python_api.workflow import Workflow
from kiara.models.module.jobs import ActiveJob
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import Operation
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.rendering import RenderValueResult
from kiara.models.values.matchers import ValueMatcher
from kiara.models.values.value import PersistedData, Value, ValueMap
from kiara.models.workflow import WorkflowGroupInfo, WorkflowInfo, WorkflowMetadata
from kiara.operations import OperationType
from kiara.operations.included_core_operations.filter import FilterOperationType
from kiara.operations.included_core_operations.pipeline import PipelineOperationDetails
from kiara.operations.included_core_operations.pretty_print import (
    PrettyPrintOperationType,
)
from kiara.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)
from kiara.registries.data import ValueLink
from kiara.registries.ids import ID_REGISTRY
from kiara.registries.operations import OP_TYPE
from kiara.utils import log_exception, log_message
from kiara.utils.operations import create_operation

if TYPE_CHECKING:
    from kiara.context import Kiara, KiaraRuntimeConfig

logger = structlog.getLogger()
yaml = YAML(typ="safe")


class KiaraAPI(object):
    """Public API for clients

    This class wraps a [Kiara][kiara.context.kiara.Kiara] instance, and allows easy a access to tasks that are
    typically done by a frontend. The return types of each method are json seriable in most cases.

    Can be extended for special scenarios and augmented with scenario-specific methdos (Jupyter, web-frontend, ...)
    ."""

    _instances: Dict[uuid.UUID, "KiaraAPI"] = {}

    @classmethod
    def instance(
        cls,
        context: Union["Kiara", str, None] = None,
        runtime_config: Union[None, Mapping[str, Any], "KiaraRuntimeConfig"] = None,
    ) -> "KiaraAPI":

        from kiara.context import Kiara

        if context is not None:
            if isinstance(context, Kiara):
                if context.id in cls._instances.keys():
                    return cls._instances[context.id]
                else:
                    api = KiaraAPI(kiara=context)
                    cls._instances[context.id] = api
                    return api

        context_obj = Kiara.instance(
            context_name=context, runtime_config=runtime_config
        )
        if context_obj.id in cls._instances.keys():
            return cls._instances[context_obj.id]
        else:
            api = KiaraAPI(kiara=context_obj)
            cls._instances[context_obj.id] = api
            return api

    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._workflow_cache: Dict[uuid.UUID, Workflow] = {}

    @cached_property
    def doc(self) -> Dict[str, str]:
        """Get the documentation for this API."""

        result = {}
        for method_name in dir(self):
            if method_name.startswith("_"):
                continue

            method = getattr(self.__class__, method_name)
            doc = inspect.getdoc(method)
            if doc is None:
                doc = "-- n/a --"
            else:
                doc = textwrap.dedent(doc)

            result[method_name] = doc

        return result

    @property
    def context(self) -> "Kiara":
        """Return the kiara context.

        DON"T USE THIS! This is going away in the production release.
        """
        return self._kiara

    @property
    def runtime_config(self) -> "KiaraRuntimeConfig":
        return self.context.runtime_config

    # ==================================================================================================================
    # methods for data_types

    @property
    def data_type_names(self) -> List[str]:
        """Get a list of all registered data types."""

        return self.context.type_registry.data_type_names

    def is_internal_data_type(self, data_type_name: str) -> bool:
        """Checks if the data type is repdominantly used internally by kiara, or whether it should be exposed to the user."""

        return self.context.type_registry.is_internal_type(
            data_type_name=data_type_name
        )

    def retrieve_data_types_info(
        self, filter: Union[str, Iterable[str], None]
    ) -> DataTypeClassesInfo:
        """Retrieve information about all data types.

        A data type is a Python class that inherits from [DataType[kiara.data_types.DataType], and it wraps a specific
        Python class that holds the actual data and provides metadata and convenience methods for managing the data internally. Data types are not directly used by users, but they are exposed in the input/output schemas of moudles and other data-related features.

        Arguments:
            filter: an optional string or (list of strings) the returned datatype ids have to match (all filters in the case of a list)

        Returns:
            an object containing all information about all data types
        """

        if filter:
            title = f"Filtered data_types: {filter}"
            data_type_names: Iterable[str] = []

            for m in self._kiara.type_registry.data_type_names:
                match = True

                for f in filter:

                    if f.lower() not in m.lower():
                        match = False
                        break

                if match:
                    data_type_names.append(m)  # type: ignore
        else:
            title = "All data types"
            data_type_names = self._kiara.type_registry.data_type_names

        data_types = {
            d: self._kiara.type_registry.get_data_type_cls(d) for d in data_type_names
        }
        data_types_info = DataTypeClassesInfo.create_from_type_items(
            kiara=self.context, group_title=title, **data_types
        )

        return data_types_info  # type: ignore

    def retrieve_data_type_info(self, data_type_name: str) -> DataTypeClassInfo:
        """Retrieve information about a specific data type.

        Arguments:
            data_type: the registered name of the data type

        Returns:
            an object containing all information about a data type
        """

        dt_cls = self.context.type_registry.get_data_type_cls(data_type_name)
        info = DataTypeClassInfo.create_from_type_class(
            kiara=self.context, type_cls=dt_cls
        )
        return info

    # ==================================================================================================================
    # methods for module and operations info
    def retrieve_module_types_info(
        self, filter: Union[None, str, Iterable[str]] = None
    ) -> ModuleTypesInfo:
        """Retrieve information for all available module types (or a filtered subset thereof).

        A module type is Python class that inherits from [KiaraModule][kiara.modules.KiaraModule], and is the basic
        building block for processing pipelines. Module types are not used directly by users, Operations are. Operations
         are instantiated modules (meaning: the module & some (optional) configuration).

        Arguments:
            filter: an optional string (or list of string) the returned module names have to match (all filters in case of list)

        Returns:
            a mapping object containing module names as keys, and information about the modules as values
        """

        if filter:
            title = f"Filtered modules: {filter}"
            module_types_names: Iterable[str] = []

            for m in self._kiara.module_registry.get_module_type_names():
                match = True

                for f in filter:

                    if f.lower() not in m.lower():
                        match = False
                        break

                if match:
                    module_types_names.append(m)  # type: ignore
        else:
            title = "All modules"
            module_types_names = self._kiara.module_registry.get_module_type_names()

        module_types = {
            n: self._kiara.module_registry.get_module_class(n)
            for n in module_types_names
        }

        module_types_info = ModuleTypesInfo.create_from_type_items(  # type: ignore
            kiara=self.context, group_title=title, **module_types
        )
        return module_types_info  # type: ignore

    def retrieve_module_type_info(self, module_type: str) -> ModuleTypeInfo:
        """Retrieve information about a specific module type.

        This can be used to retrieve information like module documentation and configuration options.

        Arguments:
            module_type: the registered name of the module

        Returns:
            an object containing all information about a module type
        """

        m_cls = self._kiara.module_registry.get_module_class(module_type)
        info = ModuleTypeInfo.create_from_type_class(kiara=self.context, type_cls=m_cls)
        return info

    def create_operation(
        self,
        module_type: str,
        module_config: Union[Mapping[str, Any], str, None] = None,
    ) -> Operation:
        """Create an [Operation][kiara.models.module.operation.Operation] instance for the specified module type and (optional) config.

        This can be used to get information about the operation itself, it's inputs & outputs schemas, documentation etc.

        Arguments:
            module_type: the registered name of the module
            module_config: (Optional) configuration for the module instance.

        Returns:
            an Operation instance (which contains all the available information about an instantiated module)
        """

        if module_config is None:
            module_config = {}
        elif isinstance(module_config, str):
            try:
                module_config = json.load(module_config)  # type: ignore
            except Exception:
                try:
                    module_config = yaml.load(module_config)  # type: ignore
                except Exception:
                    raise Exception(
                        f"Can't parse module config string: {module_config}."
                    )

        if module_type == "pipeline":
            if not module_config:
                raise Exception("Pipeline configuration can't be empty.")
            assert module_config is None or isinstance(module_config, Mapping)
            operation = create_operation("pipeline", operation_config=module_config)
            return operation
        else:
            mc = Manifest(module_type=module_type, module_config=module_config)
            module_obj = self._kiara.create_module(mc)

            return module_obj.operation

    @property
    def operation_ids(self) -> List[str]:
        """Get a list of all available operation ids."""
        return self.get_operation_ids()

    def get_operation_ids(
        self, *filters: str, include_internal: bool = False
    ) -> List[str]:
        """Get a list of all operation ids that match the specified filter.

        Arguments:
            filters: a list of filters (all filters must match the operation id for the operation to be included)
            include_internal: also return internal operations
        """

        if not filters and include_internal:
            return sorted(self.context.operation_registry.operation_ids)

        else:
            return sorted(
                self.list_operations(*filters, include_internal=include_internal).keys()
            )

    def get_operation(self, operation: str, allow_external: bool = False) -> Operation:
        """Return the operation instance with the specified id.

        This can be used to get information about a specific operation, like inputs/outputs scheman, documentation, etc.

        Arguments:
            operation: the operation id

        Returns:
            operation instance data
        """

        if not allow_external:
            op = self.context.operation_registry.get_operation(operation_id=operation)
        else:
            op = create_operation(module_or_operation=operation)
        return op

    def get_operation_info(
        self, operation: str, allow_external: bool = False
    ) -> OperationInfo:
        """Return the full information for the specified operation id.

        This is similar to the 'get_operation' method, but returns additional information. Only use this instead of
        'get_operation' if you need the additional info, as it's more expensive to get.

        Arguments:
            operation: the operation id

        Returns:
            augmented operation instance data
        """

        if not allow_external:
            op = self.context.operation_registry.get_operation(operation_id=operation)
        else:
            op = create_operation(module_or_operation=operation)
        op_info = OperationInfo.create_from_operation(kiara=self.context, operation=op)
        return op_info

    def list_operations(
        self,
        *filters: str,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
    ) -> Mapping[str, Operation]:
        """List all available values, optionally filter.

        Arguments:
            filters: the (optional) filter strings, an operation must match all of them to be included in the result
            input_types: each operation must have at least one input that matches one of the specified types
            output_types: each operation must have at least one output that matches one of the specified types
            include_internal: whether to include operations that are predominantly used internally in kiara.

        Returns:
            a dictionary with the operation id as key, and [kiara.models.module.operation.Operation] instance data as value
        """

        operations = self.context.operation_registry.operations

        if filters:
            temp = {}
            for op_id, op in operations.items():
                match = True
                for f in filters:
                    if not f:
                        continue
                    if f.lower() not in op_id.lower():
                        match = False
                        break
                if match:
                    temp[op_id] = op
            operations = temp

        if not include_internal:
            temp = {}
            for op_id, op in operations.items():
                if not op.operation_details.is_internal_operation:
                    temp[op_id] = op

            operations = temp

        if input_types:
            if isinstance(input_types, str):
                input_types = [input_types]
            temp = {}
            for op_id, op in operations.items():
                for input_type in input_types:
                    match = False
                    for schema in op.inputs_schema.values():
                        if schema.type == input_type:
                            temp[op_id] = op
                            match = True
                            break
                    if match:
                        break

            operations = temp

        if output_types:
            if isinstance(output_types, str):
                output_types = [output_types]
            temp = {}
            for op_id, op in operations.items():
                for output_type in output_types:
                    match = False
                    for schema in op.outputs_schema.values():
                        if schema.type == output_type:
                            temp[op_id] = op
                            match = True
                            break
                    if match:
                        break

            operations = temp

        return operations

    def get_operations_info(
        self,
        *filters,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
    ) -> OperationGroupInfo:
        """Retrieve information about the matching operations.

        This retrieves the same list of operations as [list_operations][kiara.interfaces.python_api.KiaraAPI.list_operations],
        but augments each result instance with additional information that might be useful in frontends.

        'OperationInfo' objects contains augmented information on top of what 'normal' [Operation][kiara.models.module.operation.Operation] objects
        hold, but they can take longer to create/resolve. If you don't need any
        of the augmented information, just use the [list_operations][kiara.interfaces.python_api.KiaraAPI.list_operations] method
        instead.

        Arguments:
            filters: the (optional) filter strings, an operation must match all of them to be included in the result
            include_internal: whether to include operations that are predominantly used internally in kiara.
            output_types: each operation must have at least one output that matches one of the specified types
            include_internal: whether to include operations that are predominantly used internally in kiara.

        Returns:
            a wrapper object containing a dictionary of items with value_id as key, and [kiara.interfaces.python_api.models.info.OperationInfo] as value
        """

        title = "Available operations"
        if filters:
            title = "Filtered operations"

        operations = self.list_operations(
            *filters,
            input_types=input_types,
            output_types=output_types,
            include_internal=include_internal,
        )

        ops_info = OperationGroupInfo.create_from_operations(
            kiara=self.context, group_title=title, **operations
        )
        return ops_info

    # ==================================================================================================================
    # methods relating to values and data

    def register_data(self, data: Any, data_type: Union[None, str] = None) -> Value:
        """Register data with kiara.

        This will create a new value instance from the data and return it. The data/value itself won't be stored
        in a store, you have to use the 'store_value' function for that.

        Arguments:
            data: the data to register
            data_type: (optional) the data type of the data. If not provided, kiara will try to infer the data type.

        Returns:
            a [kiara.models.values.value.Value] instance
        """

        if data_type is None:
            raise NotImplementedError(
                "Infering data types not implemented yet. Please provide one manually."
            )

        value = self.context.data_registry.register_data(
            data=data, schema=data_type, reuse_existing=False
        )
        return value

    def get_value_ids(self, **matcher_params) -> List[uuid.UUID]:
        """List all available value ids for this kiara context.

        This method exists mainly so frontend can retrieve a list of all value_ids that exists on the backend without
        having to look up the details of each value (like [list_values][kiara.interfaces.python_api.KiaraAPI.list_values]
        does). This method can also be used with a matcher, but in this case the [list_values][kiara.interfaces.python_api.KiaraAPI.list_values]
        would be preferable in most cases, because it is called under the hood, and the performance advantage of not
        having to look up value details is gone.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a list of value ids
        """

        if matcher_params:
            values = self.list_values(**matcher_params)
            return sorted(values.keys())
        else:
            _values = self._kiara.data_registry.retrieve_all_available_value_ids()
            return sorted(_values)

    def list_values(self, **matcher_params: Any) -> Dict[uuid.UUID, Value]:
        """List all available values, optionally filter.

        Retrieve information about all values that are available in the current kiara context session (both stored
        and non-stored).

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a dictionary with value_id as key, and [kiara.models.values.value.Value] as value
        """

        if matcher_params:
            matcher = ValueMatcher.create_matcher(**matcher_params)

            values = self._kiara.data_registry.find_values(matcher=matcher)
        else:
            # TODO: make that parallel?
            values = {
                k: self._kiara.data_registry.get_value(k)
                for k in self._kiara.data_registry.retrieve_all_available_value_ids()
            }

        return values

    def get_value(self, value: Union[str, ValueLink, uuid.UUID]) -> Value:
        """Retrieve a value instance with the specified id or alias.

        Raises an exception if no value could be found.

        Arguments:
            value: a value id, alias or object that has a 'value_id' attribute.

        Returns:
            the Value instance
        """

        return self._kiara.data_registry.get_value(value=value)

    def get_value_info(self, value: Union[str, uuid.UUID, ValueLink]) -> ValueInfo:
        """Retrieve an info object for a value.

        'ValueInfo' objects contains augmented information on top of what 'normal' [Value][kiara.models.values.value.Value] objects
        hold (like resolved properties for example), but they can take longer to create/resolve. If you don't need any
        of the augmented information, just use the [get_value][kiara.interfaces.python_api.KiaraAPI.get_value] method
        instead.

        Arguments:
            value: a value id, alias or object that has a 'value_id' attribute.

        Returns:
            the ValueInfo instance

        """

        _value = self.get_value(value=value)
        return ValueInfo.create_from_instance(kiara=self._kiara, instance=_value)

    def get_values_info(self, **matcher_params) -> ValuesInfo:
        """Retrieve information about the matching values.

        This retrieves the same list of values as [list_values][kiara.interfaces.python_api.KiaraAPI.list_values],
        but augments each result value instance with additional information that might be useful in frontends.

        'ValueInfo' objects contains augmented information on top of what 'normal' [Value][kiara.models.values.value.Value] objects
        hold (like resolved properties for example), but they can take longer to create/resolve. If you don't need any
        of the augmented information, just use the [list_values][kiara.interfaces.python_api.KiaraAPI.list_values] method
        instead.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a wrapper object containing the items as dictionary with value_id as key, and [kiara.interfaces.python_api.models.values.ValueInfo] as value
        """

        values = self.list_values(**matcher_params)

        infos = ValuesInfo.create_from_instances(
            kiara=self._kiara, instances={str(k): v for k, v in values.items()}
        )
        return infos  # type: ignore

    def get_alias_names(self, **matcher_params) -> List[str]:
        """List all available alias keys.

        This method exists mainly so frontend can retrieve a list of all value_ids that exists on the backend without
        having to look up the details of each value (like [list_aliases][kiara.interfaces.python_api.KiaraAPI.list_aliases]
        does). This method can also be used with a matcher, but in this case the [list_aliases][kiara.interfaces.python_api.KiaraAPI.list_aliases]
        would be preferrable in most cases, because it is called under the hood, and the performance advantage of not
        having to look up value details is gone.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a list of value ids
        """

        if matcher_params:
            values = self.list_aliases(**matcher_params)
            return list(values.keys())
        else:
            _values = self._kiara.alias_registry.all_aliases
            return list(_values)

    def list_aliases(self, **matcher_params) -> Dict[str, Value]:
        """List all available values that have an alias assigned, optionally filter.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a dictionary with value_id as key, and [kiara.models.values.value.Value] as value
        """

        if matcher_params:
            matcher_params["has_alias"] = True
            all_values = self.list_values(**matcher_params)
            result: Dict[str, Value] = {}
            for value in all_values.values():
                aliases = self._kiara.alias_registry.find_aliases_for_value_id(
                    value_id=value.value_id
                )
                for a in aliases:
                    if a in result.keys():
                        raise Exception(
                            f"Duplicate value alias '{a}': this is most likely a bug."
                        )
                    result[a] = value

            result = {k: result[k] for k in sorted(result.keys())}
        else:
            # faster if not other matcher params
            all_aliases = self._kiara.alias_registry.all_aliases
            result = {
                k: self._kiara.data_registry.get_value(f"alias:{k}")
                for k in all_aliases
            }

        return result

    def get_aliases_info(self, **matcher_params) -> ValuesInfo:
        """Retrieve information about the matching values.

        This retrieves the same list of values as [list_values][kiara.interfaces.python_api.KiaraAPI.list_values],
        but augments each result value instance with additional information that might be useful in frontends.

        'ValueInfo' objects contains augmented information on top of what 'normal' [Value][kiara.models.values.value.Value] objects
        hold (like resolved properties for example), but they can take longer to create/resolve. If you don't need any
        of the augmented information, just use the [get_value][kiara.interfaces.python_api.KiaraAPI.list_aliases] method
        instead.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a dictionary with a value alias as key, and [kiara.interfaces.python_api.models.values.ValueInfo] as value
        """

        values = self.list_aliases(**matcher_params)

        infos = ValuesInfo.create_from_instances(
            kiara=self._kiara, instances={str(k): v for k, v in values.items()}
        )
        return infos  # type: ignore

    def store_value(
        self,
        value: Union[str, uuid.UUID, ValueLink],
        aliases: Union[str, Iterable[str], None],
    ) -> StoreValueResult:
        """Store the specified value in the (default) value store.

        Arguments:
            value: the value (or a reference to it)
            aliases: (Optional) aliases for the value
        """

        if isinstance(aliases, str):
            aliases = [aliases]

        value_obj = self.get_value(value)
        persisted_data: Union[None, PersistedData] = None
        try:
            persisted_data = self.context.data_registry.store_value(value=value_obj)
            if aliases:
                self.context.alias_registry.register_aliases(
                    value_obj.value_id, *aliases
                )
            result = StoreValueResult.construct(
                value=value_obj,
                aliases=sorted(aliases) if aliases else [],
                error=None,
                persisted_data=persisted_data,
            )
        except Exception as e:
            log_exception(e)
            result = StoreValueResult.construct(
                value=value_obj,
                aliases=sorted(aliases) if aliases else [],
                error=str(e),
                persisted_data=persisted_data,
            )

        return result

    def store_values(
        self,
        values: Mapping[str, Union[str, uuid.UUID, ValueLink]],
        alias_map: Mapping[str, Iterable[str]],
    ) -> StoreValuesResult:
        """Store multiple values into the (default) kiara value store.

        Values are identified by unique keys in both input arguments, the alias map references the key that is used in
        the 'values' argument.

        Arguments:
            values: a map of value keys/values
            alias_map: a map of value keys aliases

        Returns:
            an object outlining which values (identified by the specified value key) where stored and how
        """

        result = {}
        for field_name, value in values.items():
            aliases = alias_map.get(field_name)
            value_obj = self.get_value(value)
            store_result = self.store_value(value=value_obj, aliases=aliases)
            result[field_name] = store_result

        return StoreValuesResult.construct(__root__=result)

    # ------------------------------------------------------------------------------------------------------------------
    # operation-related methods

    def get_operation_type(self, op_type: Union[str, Type[OP_TYPE]]) -> OperationType:
        """Get the management object for the specified operation type."""

        return self.context.operation_registry.get_operation_type(op_type=op_type)

    def get_operation_type_info(
        self, op_type: Union[str, Type[OP_TYPE]]
    ) -> OperationTypeInfo:
        """Get an info object for the specified operation type."""

        _op_type = self.get_operation_type(op_type=op_type)
        return OperationTypeInfo.create_from_type_class(
            kiara=self.context, type_cls=_op_type.__class__
        )

    def find_operation_id(
        self, module_type: str, module_config: Union[None, Mapping[str, Any]] = None
    ) -> Union[None, str]:
        """Try to find the registered operation id for the specified module type and configuration.

        Arguments:
            module_type: the module type
            module_config: the module configuration

        Returns:
            the registered operation id, if found, or None
        """

        manifest = self.context.create_manifest(
            module_or_operation=module_type, config=module_config
        )
        return self.context.operation_registry.find_operation_id(manifest=manifest)

    def assemble_filter_pipeline_config(
        self,
        data_type: str,
        filters: Union[str, Iterable[str], Mapping[str, str]],
        endpoint: Union[None, Manifest, str] = None,
        endpoint_input_field: Union[str, None] = None,
        endpoint_step_id: Union[str, None] = None,
        extra_input_aliases: Union[None, Mapping[str, str]] = None,
        extra_output_aliases: Union[None, Mapping[str, str]] = None,
    ) -> PipelineConfig:
        """Assemble a (pipeline) module config to filter values of a specific data type.

        Optionally, a module that uses the filtered dataset as input can be specified.

        # TODO: document filter names
        For the 'filters' argument, the accepted inputs are:
        - a string, in which case a single-step pipeline will be created, with the string referencing the operation id or filter
        - a list of strings: in which case a multi-step pipeline will be created, the step_ids will be calculated automatically
        - a map of string pairs: the keys are step ids, the values operation ids or filter names

        Arguments:
            data_type: the type of the data to filter
            filters: a list of operation ids or filter names (and potentiall step_ids if type is a mapping)
            endpoint: optional module to put as last step in the created pipeline
            endpoing_input_field: field name of the input that will receive the filtered value
            endpoint_step_id: id to use for the endpoint step (module type name will be used if not provided)
            extra_input_aliases: extra output aliases to add to the pipeline config
            extra_output_aliases: extra output aliases to add to the pipeline config

        Returns:
            the (pipeline) module configuration of the filter pipeline
        """

        filter_op_type: FilterOperationType = self._kiara.operation_registry.get_operation_type("filter")  # type: ignore
        pipeline_config = filter_op_type.assemble_filter_pipeline_config(
            data_type=data_type,
            filters=filters,
            endpoint=endpoint,
            endpoint_input_field=endpoint_input_field,
            endpoint_step_id=endpoint_step_id,
            extra_input_aliases=extra_input_aliases,
            extra_output_aliases=extra_output_aliases,
        )

        return pipeline_config

    def assemble_render_pipeline(
        self,
        data_type: str,
        target_format: Union[str, Iterable[str]] = "string",
        filters: Union[None, str, Iterable[str], Mapping[str, str]] = None,
        use_pretty_print: bool = False,
    ) -> Operation:
        """Create a manifest describing a transformation that renders a value of the specified data type in the target format.

        If a list is provided as value for 'target_format', all items are tried until a 'render_value' operation is found that matches
        the value type of the source value, and the provided target format.

        Arguments:
            value: the value (or value id)
            target_format: the format into which to render the value
            filters: a list of filters to apply to the value before rendering it
            use_pretty_print: if True, use a 'pretty_print' operation instead of 'render_value'

        Returns:
            the manifest for the transformation
        """

        if data_type not in self.context.data_type_names:
            raise DataTypeUnknownException(data_type=data_type)

        if use_pretty_print:
            pretty_print_op_type: PrettyPrintOperationType = (
                self._kiara.operation_registry.get_operation_type("pretty_print")
            )  # type: ignore
            ops = pretty_print_op_type.get_target_types_for(data_type)
        else:
            render_op_type: RenderValueOperationType = self._kiara.operation_registry.get_operation_type(
                # type: ignore
                "render_value"
            )  # type: ignore
            ops = render_op_type.get_render_operations_for_source_type(data_type)

        if isinstance(target_format, str):
            target_format = [target_format]

        match = None
        for _target_type in target_format:
            if _target_type not in ops.keys():
                continue
            match = ops[_target_type]
            break

        if not match:
            if not ops:
                msg = f"No render operations registered for source type '{data_type}'."
            else:
                msg = f"Registered target types for source type '{data_type}': {', '.join(ops.keys())}."
            raise Exception(
                f"No render operation for source type '{data_type}' to target type(s) registered: '{', '.join(target_format)}'. {msg}"
            )

        if filters:
            # filter_op_type: FilterOperationType = self._kiara.operation_registry.get_operation_type("filter")  # type: ignore
            endpoint = Manifest(
                module_type=match.module_type, module_config=match.module_config
            )
            extra_input_aliases = {"render_value.render_config": "render_config"}
            extra_output_aliases = {
                "render_value.render_value_result": "render_value_result"
            }
            pipeline_config = self.assemble_filter_pipeline_config(
                data_type=data_type,
                filters=filters,
                endpoint=endpoint,
                endpoint_input_field="value",
                endpoint_step_id="render_value",
                extra_input_aliases=extra_input_aliases,
                extra_output_aliases=extra_output_aliases,
            )
            manifest = Manifest(
                module_type="pipeline", module_config=pipeline_config.dict()
            )
            module = self._kiara.module_registry.create_module(manifest=manifest)
            operation = Operation.create_from_module(module, doc=pipeline_config.doc)
        else:
            operation = match

        return operation

    # ------------------------------------------------------------------------------------------------------------------
    # job-related methods
    def queue_manifest(
        self, manifest: Manifest, inputs: Union[None, Mapping[str, Any]] = None
    ) -> uuid.UUID:
        """Queue a job using the provided manifest to describe the module and config that should be executed.

        Arguments:
            manifest: the manifest
            inputs: the job inputs (can be either references to values, or raw inputs

        Returns:
            a result value map instance
        """

        if inputs is None:
            inputs = {}
        job_config = self.context.job_registry.prepare_job_config(
            manifest=manifest, inputs=inputs
        )

        job_id = self.context.job_registry.execute_job(
            job_config=job_config, wait=False
        )
        return job_id

    def run_manifest(
        self, manifest: Manifest, inputs: Union[None, Mapping[str, Any]] = None
    ) -> ValueMap:
        """Run a job using the provided manifest to describe the module and config that should be executed.

        Arguments:
            manifest: the manifest
            inputs: the job inputs (can be either references to values, or raw inputs

        Returns:
            a result value map instance
        """

        job_id = self.queue_manifest(manifest=manifest, inputs=inputs)
        return self.context.job_registry.retrieve_result(job_id=job_id)

    def queue_job(
        self,
        operation: Union[str, Path, Manifest, OperationInfo],
        inputs: Mapping[str, Any],
        operation_config: Union[None, Mapping[str, Any]] = None,
    ) -> uuid.UUID:
        """Queue a job from a operation id, module_name (and config), or pipeline file, wait for the job to finish and retrieve the result.

        This is a convenience method that auto-detects what is meant by the 'operation' string input argument.

        Arguments:
            operation: a module name, operation id, or a path to a pipeline file (resolved in this order, until a match is found)..
            inputs: the operation inputs
            operation_config: the (optional) module config in case 'operation' is a module name

        Returns:
            the queued job id
        """

        if isinstance(operation, Path):
            if not operation.is_file():
                raise Exception(
                    f"Can't queue job from file '{operation.as_posix()}': file does not exist."
                )
            operation = operation.as_posix()
        elif isinstance(operation, OperationInfo):
            operation = operation.operation

        if not isinstance(operation, Manifest):
            manifest: Manifest = create_operation(
                module_or_operation=operation,
                operation_config=operation_config,
                kiara=self.context,
            )
        else:
            manifest = operation

        job_id = self.queue_manifest(manifest=manifest, inputs=inputs)
        return job_id

    def run_job(
        self,
        operation: Union[str, Path, Manifest, OperationInfo],
        inputs: Mapping[str, Any],
        operation_config: Union[None, Mapping[str, Any]] = None,
    ) -> ValueMap:
        """Run a job from a operation id, module_name (and config), or pipeline file, wait for the job to finish and retrieve the result.

        This is a convenience method that auto-detects what is meant by the 'operation' string input argument.

        In general, try to avoid this method and use 'queue_job', 'get_job' and 'retrieve_job_result' manually instead,
        since this is a blocking operation.

        Arguments:
            operation: a module name, operation id, or a path to a pipeline file (resolved in this order, until a match is found)..
            inputs: the operation inputs
            operation_config: the (optional) module config in case 'operation' is a module name

        Returns:
            the job result value map

        """
        job_id = self.queue_job(
            operation=operation, inputs=inputs, operation_config=operation_config
        )
        return self.context.job_registry.retrieve_result(job_id=job_id)

    def get_job(self, job_id: Union[str, uuid.UUID]) -> ActiveJob:
        """Retrieve the status of the job with the provided id."""

        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)

        job_status = self.context.job_registry.get_job(job_id=job_id)
        return job_status

    def retrieve_job_result(self, job_id: Union[str, uuid.UUID]) -> ValueMap:
        """Retrieve the result(s) of the specified job."""

        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)

        result = self.context.job_registry.retrieve_result(job_id=job_id)
        return result

    def render_value(
        self,
        value: Union[str, uuid.UUID, ValueLink],
        target_format: Union[str, Iterable[str]] = "string",
        filters: Union[None, Iterable[str], Mapping[str, str]] = None,
        render_config: Union[Mapping[str, str], None] = None,
        add_root_scenes: bool = True,
        use_pretty_print: bool = False,
    ) -> RenderValueResult:
        """Render a value in the specified target format.

        If a list is provided as value for 'target_format', all items are tried until a 'render_value' operation is found that matches
        the value type of the source value, and the provided target format.

        Arguments:
            value: the value (or value id)
            target_format: the format into which to render the value
            filters: an (optional) list of filters
            render_config: manifest specific render configuration
            add_root_scenes: add root scenes to the result
            use_pretty_print: use 'pretty_print' operation instead of 'render_value'

        Returns:
            the rendered value data, and any related scenes, if applicable
        """

        _value = self.get_value(value)
        try:
            render_operation: Union[None, Operation] = self.assemble_render_pipeline(
                data_type=_value.data_type_name,
                target_format=target_format,
                filters=filters,
                use_pretty_print=True,
            )
        except Exception as e:
            log_message(
                "create_render_pipeline.failure",
                source_type=_value.data_type_name,
                target_format=target_format,
                error=e,
            )

            if use_pretty_print:
                pretty_print_ops: PrettyPrintOperationType = self.context.operation_registry.get_operation_type("pretty_print")  # type: ignore
                if not isinstance(target_format, str):
                    raise NotImplementedError(
                        "Can't handle multiple target formats for 'render_value' yet."
                    )
                render_operation = (
                    pretty_print_ops.get_operation_for_render_combination(
                        source_type="any", target_type=target_format
                    )
                )
            else:
                render_ops: RenderValueOperationType = self.context.operation_registry.get_operation_type("render_value")  # type: ignore
                if not isinstance(target_format, str):
                    raise NotImplementedError(
                        "Can't handle multiple target formats for 'render_value' yet."
                    )
                render_operation = render_ops.get_render_operation(
                    source_type="any", target_type=target_format
                )

        if render_operation is None:
            raise Exception(
                f"Could not find render operation for value: {_value.value_id}"
            )

        if render_config and "render_config" in render_config.keys():
            # raise NotImplementedError()
            # TODO: is this necessary?
            render_config = render_config["render_config"]  # type: ignore
            # manifest_hash = render_config["manifest_hash"]
            # if manifest_hash != render_operation.manifest_hash:
            #     raise NotImplementedError(
            #         "Using a non-default render operation is not supported (yet)."
            #     )
            # render_config = render_config["render_config"]

        if render_config is None:
            render_config = {}
        else:
            render_config = dict(render_config)

        # render_type = render_config.pop("render_type", None)
        # if not render_type or render_type == "data":
        #     pass
        # elif render_type == "metadata":
        #     pass
        # elif render_type == "properties":
        #     pass
        # elif render_type == "lineage":
        #     pass

        result = render_operation.run(
            kiara=self.context,
            inputs={"value": _value, "render_config": render_config},
        )

        if use_pretty_print:
            render_result: Value = result["rendered_value"]
            value_render_data = render_result.data
        else:
            render_result = result["render_value_result"]

            if render_result.data_type_name != "render_value_result":
                raise Exception(
                    f"Invalid result type for render operation: {render_result.data_type_name}"
                )

            value_render_data: RenderValueResult = render_result.data  # type: ignore

        return value_render_data

    # ------------------------------------------------------------------------------------------------------------------
    # workflow-related methods
    @property
    def workflow_ids(self) -> List[uuid.UUID]:
        """List all available workflow ids."""

        return list(self.context.workflow_registry.all_workflow_ids)

    @property
    def workflow_aliases(self) -> List[str]:
        """ "List all available workflow aliases."""

        return list(self.context.workflow_registry.workflow_aliases.keys())

    def get_workflow(
        self, workflow: Union[str, uuid.UUID], create_if_necessary: bool = True
    ) -> Workflow:
        """Retrieve the workflow instance with the specified id or alias."""

        no_such_alias: bool = False
        workflow_id: Union[uuid.UUID, None] = None
        workflow_alias: Union[str, None] = None

        if isinstance(workflow, str):
            try:
                workflow_id = uuid.UUID(workflow)
            except Exception:
                workflow_alias = workflow
                try:
                    workflow_id = self.context.workflow_registry.get_workflow_id(
                        workflow_alias=workflow
                    )
                except NoSuchWorkflowException:
                    no_such_alias = True
        else:
            workflow_id = workflow

        if workflow_id is None:
            raise Exception(f"Can't retrieve workflow for: {workflow}")

        if workflow_id in self._workflow_cache.keys():
            return self._workflow_cache[workflow_id]

        if workflow_id is None and not create_if_necessary:
            if not no_such_alias:
                msg = f"No workflow with id '{workflow}' registered."
            else:
                msg = f"No workflow with alias '{workflow}' registered."

            raise NoSuchWorkflowException(workflow=workflow, msg=msg)

        if workflow_id:
            # workflow_metadata = self.context.workflow_registry.get_workflow_metadata(
            #     workflow=workflow_id
            # )
            workflow_obj = Workflow(kiara=self.context, workflow=workflow_id)
            self._workflow_cache[workflow_obj.workflow_id] = workflow_obj
        else:
            # means we need to create it
            workflow_obj = self.create_workflow(workflow_alias=workflow_alias)

        return workflow_obj

    def get_workflow_info(self, workflow: Union[str, uuid.UUID, Workflow]):

        if isinstance(workflow, Workflow):
            _workflow: Workflow = workflow
        else:
            _workflow = self.get_workflow(workflow)

        return WorkflowInfo.create_from_workflow(workflow=_workflow)

    def list_workflows(self, **matcher_params) -> Mapping[uuid.UUID, Workflow]:
        """List all available workflow sessions, indexed by their unique id."""

        workflows = {}

        matcher = WorkflowMatcher(**matcher_params)
        if matcher.has_alias:
            for (
                alias,
                workflow_id,
            ) in self.context.workflow_registry.workflow_aliases.items():

                workflow = self.get_workflow(workflow=workflow_id)
                workflows[workflow.workflow_id] = workflow
            return workflows
        else:
            for workflow_id in self.context.workflow_registry.all_workflow_ids:
                workflow = self.get_workflow(workflow=workflow_id)
                workflows[workflow_id] = workflow
            return workflows

    def list_workflow_aliases(self, **matcher_params) -> Dict[str, Workflow]:
        """List all available workflow sessions that have an alias, indexed by alias."""

        if matcher_params:
            matcher_params["has_alias"] = True
            workflows = self.list_workflows(**matcher_params)
            result: Dict[str, Workflow] = {}
            for workflow in workflows.values():
                aliases = self.context.workflow_registry.get_aliases(
                    workflow_id=workflow.workflow_id
                )
                for a in aliases:
                    if a in result.keys():
                        raise Exception(
                            f"Duplicate workflow alias '{a}': this is most likely a bug."
                        )
                    result[a] = workflow
            result = {k: result[k] for k in sorted(result.keys())}
        else:
            # faster if not other matcher params
            all_aliases = self.context.workflow_registry.workflow_aliases
            result = {
                a: self.get_workflow(workflow=all_aliases[a])
                for a in sorted(all_aliases.keys())
            }
        return result

    def get_workflows_info(self, **matcher_params: Any) -> WorkflowGroupInfo:
        """Get a map info instances for all available workflows, indexed by (stringified) workflow-id."""

        workflows = self.list_workflows(**matcher_params)

        workflow_infos = WorkflowGroupInfo.create_from_workflows(
            *workflows.values(),
            group_title=None,
            alias_map=self.context.workflow_registry.workflow_aliases,
        )
        return workflow_infos

    def get_workflow_aliases_info(self, **matcher_params: Any) -> WorkflowGroupInfo:
        """Get a map info instances for all available workflows, indexed by alias."""

        workflows = self.list_workflow_aliases(**matcher_params)
        workflow_infos = WorkflowGroupInfo.create_from_workflows(
            *workflows.values(),
            group_title=None,
            alias_map=self.context.workflow_registry.workflow_aliases,
        )
        return workflow_infos

    def create_workflow(
        self,
        workflow_alias: Union[None, str] = None,
        initial_pipeline: Union[None, str] = None,
        initial_inputs: Union[None, Mapping[str, Any]] = None,
        documentation: Union[Any, None] = None,
        save: bool = False,
        force_alias: bool = False,
    ) -> Workflow:

        if workflow_alias is not None:
            try:
                uuid.UUID(workflow_alias)
                raise Exception(
                    f"Can't create workflow, provided alias can't be a uuid: {workflow_alias}."
                )
            except Exception:
                pass

        workflow_id = ID_REGISTRY.generate()
        metadata = WorkflowMetadata(
            workflow_id=workflow_id, documentation=documentation
        )

        workflow_obj = Workflow(kiara=self.context, workflow=metadata)
        if workflow_alias:
            workflow_obj._pending_aliases.add(workflow_alias)

        if initial_pipeline:
            operation = self.get_operation(
                operation=initial_pipeline, allow_external=True
            )
            if operation.module_type == "pipeline":
                pipeline_details: PipelineOperationDetails = operation.operation_details  # type: ignore
                workflow_obj.add_steps(*pipeline_details.pipeline_config.steps)
                input_aliases = pipeline_details.pipeline_config.input_aliases
                for k, v in input_aliases.items():
                    workflow_obj.set_input_alias(input_field=k, alias=v)
                output_aliases = pipeline_details.pipeline_config.output_aliases
                for k, v in output_aliases.items():
                    workflow_obj.set_output_alias(output_field=k, alias=v)
            else:
                raise NotImplementedError()

            workflow_obj.set_inputs(**operation.module.config.defaults)

        if initial_inputs:
            workflow_obj.set_inputs(**initial_inputs)

        self._workflow_cache[workflow_obj.workflow_id] = workflow_obj

        if save:
            if force_alias and workflow_alias:
                self.context.workflow_registry.unregister_alias(workflow_alias)
            workflow_obj.save()

        return workflow_obj
