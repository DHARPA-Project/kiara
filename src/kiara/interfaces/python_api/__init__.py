# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import structlog
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Type, Union

from kiara.interfaces.python_api.models.info import (
    ModuleTypeInfo,
    ModuleTypesInfo,
    OperationGroupInfo,
    OperationInfo,
    ValueInfo,
    ValuesInfo,
)
from kiara.interfaces.python_api.value import StoreValueResult, StoreValuesResult
from kiara.models.module.jobs import ActiveJob
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import Operation
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.rendering import RenderValueResult
from kiara.models.values.matchers import ValueMatcher
from kiara.models.values.value import PersistedData, Value, ValueMap
from kiara.operations.included_core_operations.filter import FilterOperationType
from kiara.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)
from kiara.registries.data import ValueLink
from kiara.registries.operations import OP_TYPE
from kiara.utils import log_exception
from kiara.utils.operations import create_operation

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()


class KiaraAPI(object):
    """Public API for clients

    This class wraps a [Kiara][kiara.context.kiara.Kiara] instance, and allows easy a access to tasks that are
    typically done by a frontend. The return types of each method are json seriable in most cases.

    Can be extended for special scenarios and augmented with scenario-specific methdos (Jupyter, web-frontend, ...)
    ."""

    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

    @property
    def context(self) -> "Kiara":
        """Return the kiara context.

        DON"T USE THIS! This is going away in the production release.
        """
        return self._kiara

    # ==================================================================================================================
    # methods for module and operations info
    def retrieve_module_types_info(
        self, filter: Union[None, str, Iterable[str]] = None
    ) -> ModuleTypesInfo:
        """Retrieve information for all available module types (or a filtered subset thereof).

        Arguments:
            filter: a string (or list of string) the returned module names have to match (all filters in case of list)

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

        Arguments:
            module_type: the registered name of the module

        Returns:
            an object containing all information about a module type
        """

        m_cls = self._kiara.module_registry.get_module_class(module_type)
        info = ModuleTypeInfo.create_from_type_class(kiara=self.context, type_cls=m_cls)
        return info

    def create_operation(
        self, module_type: str, module_config: Union[Mapping[str, Any], None] = None
    ) -> Operation:
        """Create an Operation instance.

        Arguments:
            module_type: the registered name of the module
            module_config: (Optional) configuration for the module instance.

        Returns:
            an Operation instance (which contains all the available information about an instantiated module)
        """

        if module_config is None:
            module_config = {}

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

    def get_operation(self, operation_id: str) -> Operation:
        """Return the operation instance with the specified id.

        Arguments:
            operation_id: the operation id
        """

        return self.context.operation_registry.get_operation(operation_id=operation_id)

    def get_operation_info(self, operation_id: str) -> OperationInfo:
        """Return the full information for the specified operation id.

        This is similar to the 'get_operation' method, but returns additional information. Only use this instead of
        'get_operation' if you need the additional info, as it's more expensive to get.

        Arguments:
            operation_id: the operation id
        """

        op = self.context.operation_registry.get_operation(operation_id=operation_id)
        op_info = OperationInfo.create_from_operation(kiara=self.context, operation=op)
        return op_info

    def list_operations(
        self, *filters: str, include_internal: bool = False
    ) -> Mapping[str, Operation]:
        """List all available values, optionally filter.

        Arguments:
            filters: the (optional) filter strings, an operation must match all of them to be included in the result
            include_internal: whether to include operations that are predominantly used internally in kiara.

        Returns:
            a dictionary with the operation id as key, and a [kiara.models.module.operation.Operation] instance as value
        """

        operations = self.context.operation_registry.operations

        if filters:
            temp = {}
            for op_id, op in operations.items():
                match = True
                for f in filters:
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

        return operations

    def get_operations_info(
        self, *filters, include_internal: bool = False
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

        Returns:
            a wrapper object containing a dictionary of items with value_id as key, and [kiara.interfaces.python_api.models.info.OperationInfo] as value
        """

        title = "Available operations"
        if filters:
            title = "Filtered operations"

        operations = self.list_operations(*filters, include_internal=include_internal)

        ops_info = OperationGroupInfo.create_from_operations(
            kiara=self.context, group_title=title, **operations
        )
        return ops_info

    # ==================================================================================================================
    # methods relating to values and data

    def get_value_ids(self, **matcher_params) -> List[uuid.UUID]:
        """List all available value ids.

        This method exists mainly so frontend can retrieve a list of all value_ids that exists on the backend without
        having to look up the details of each value (like [list_values][kiara.interfaces.python_api.KiaraAPI.list_values]
        does). This method can also be used with a matcher, but in this case the [list_values][kiara.interfaces.python_api.KiaraAPI.list_values]
        would be preferrable in most cases, because it is called under the hood, and the performance advantage of not
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
    ):
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

    def get_operation_type(self, op_type: Union[str, Type[OP_TYPE]]):
        """Get the management object for the specified operation type."""

        return self.context.operation_registry.get_operation_type(op_type=op_type)

    # ------------------------------------------------------------------------------------------------------------------
    # pipeline-related methods

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
    ) -> Operation:
        """Create a manifest describing a transformation that renders a value of the specified data type in the target format.

        If a list is provided as value for 'target_format', all items are tried until a 'render_value' operation is found that matches
        the value type of the source value, and the provided target format.

        Arguments:
            value: the value (or value id)
            target_format: the format into which to render the value

        Returns:
            the manifest for the transformation
        """

        render_op_type: RenderValueOperationType = self._kiara.operation_registry.get_operation_type(  # type: ignore
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
        operation: Union[str, Path, Manifest],
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
        operation: Union[str, Path, Manifest],
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
    ) -> RenderValueResult:
        """Render a value in the specified target format.

        If a list is provided as value for 'target_format', all items are tried until a 'render_value' operation is found that matches
        the value type of the source value, and the provided target format.

        Arguments:
            value: the value (or value id)
            target_format: the format into which to render the value
            filters: an (optional) list of filters
            render_config: manifest specific render configuration

        Returns:
            the rendered value data
        """

        _value = self.get_value(value)
        render_operation = self.assemble_render_pipeline(
            data_type=_value.data_type_name,
            target_format=target_format,
            filters=filters,
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

        result = render_operation.run(
            kiara=self.context,
            inputs={"value": _value, "render_config": render_config},
        )

        render_result = result["render_value_result"]
        if render_result.data_type_name != "render_value_result":
            raise Exception(
                f"Invalid result type for render operation: {render_result.data_type_name}"
            )

        return render_result.data
