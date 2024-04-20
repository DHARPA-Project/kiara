# -*- coding: utf-8 -*-
import uuid
from pathlib import Path

# BEGIN AUTO-GENERATED-IMPORTS
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, List, Mapping, Union
from uuid import UUID

if TYPE_CHECKING:
    from kiara.interfaces.python_api.models.info import (
        DataTypeClassesInfo,
        DataTypeClassInfo,
        KiaraPluginInfo,
        KiaraPluginInfos,
        ModuleTypeInfo,
        ModuleTypesInfo,
        OperationGroupInfo,
        OperationInfo,
        ValueInfo,
        ValuesInfo,
    )
    from kiara.interfaces.python_api.value import StoreValueResult, StoreValuesResult
    from kiara.models.context import ContextInfo, ContextInfos
    from kiara.models.module.operation import Operation
    from kiara.models.values.value import Value, ValueMapReadOnly

# END AUTO-GENERATED-IMPORTS

if TYPE_CHECKING:
    from kiara.context import KiaraConfig
    from kiara.interfaces.python_api.models.archive import KiArchive
    from kiara.interfaces.python_api.models.doc import OperationsMap
    from kiara.interfaces.python_api.models.info import (
        KiaraPluginInfo,
        KiaraPluginInfos,
    )
    from kiara.interfaces.python_api.models.job import JobDesc
    from kiara.models.archives import KiArchiveInfo
    from kiara.models.metadata import KiaraMetadata
    from kiara.models.module.jobs import ActiveJob, JobRecord
    from kiara.models.module.manifest import Manifest


class KiaraAPI(object):
    """Kiara API for clients.

    This class wraps a [Kiara][kiara.context.kiara.Kiara] instance, and allows easy a access to tasks that are
    typically done by a frontend. The return types of each method are json seriable in most cases.

    The naming of the API endpoints follows a (loose-ish) convention:
    - list_*: return a list of ids or items, if items, filtering is supported
    - get_*: get specific instances of a type (operation, value, etc.)
    - retrieve_*: get augmented information about an instance or type of something. This usually implies that there is some overhead,
    so before you use this, make sure that there is not 'get_*' or 'list_*' endpoint that could give you what you need.

    Some methods of this class are copied (automatically) from the [BaseAPI][kiara.interfaces.python_api.base_api.BaseAPI] class, which is the actual implementation of the API.
    This is done for different reasons:
    - to keep the 'BaseAPI' class flexible, as it is used internally, so the `KiaraAPI` class can serve as a sort of 'stable' frontend, even if the underlying BaseAPI changes
    - to avoid having to write the same documentation / code twice
    - to be able to postpone the imports that are in the `base_api` module
    - to be able to add instrumentation, logging, etc. to the API calls later on

    Re-generating those copied methods can be done like so:

    ```
     kiara render --source-type base_api --target-type kiara_api item kiara_api template_file=kiara/src/kiara/interfaces/python_api/kiara_api.py target_file=kiara/src/kiara/interfaces/python_api/kiara_api.py
    ```

    All endpoints that have the 'tag' annotation `kiara_api` will then be copied.

    """

    _default_instance: ClassVar[Union["KiaraAPI", None]] = None

    @classmethod
    def instance(cls) -> "KiaraAPI":
        """Retrieve the default KiaraAPI instance.

        This is a convenience method to get a singleton KiaraAPI instance. If this is the first time this method is called, it loads the default *kiara* context. If this is called subsequently, it will return
        the same instance, so if you or some-one (or -thing) switched that context, this might not be the case.

        So make sure you understand the implications, and if in doubt, it might be safer to create your own `KiaraAPI` instance manually.
        """

        if cls._default_instance is not None:
            return cls._default_instance

        from kiara.utils.config import assemble_kiara_config

        config = assemble_kiara_config()

        api = KiaraAPI(kiara_config=config)
        cls._default_instance = api
        return api

    def __init__(self, kiara_config: Union["KiaraConfig", None] = None):

        from kiara.interfaces.python_api.base_api import BaseAPI

        self._api: BaseAPI = BaseAPI(kiara_config=kiara_config)

    def run_job(
        self,
        operation: Union[str, Path, "Manifest", "OperationInfo", "JobDesc"],
        inputs: Union[Mapping[str, Any], None] = None,
        comment: Union[str, None] = None,
        operation_config: Union[None, Mapping[str, Any]] = None,
    ) -> "ValueMapReadOnly":
        """
        Run a job from a operation id, module_name (and config), or pipeline file, wait for the job to finish and retrieve the result.

        This is a convenience method that auto-detects what is meant by the 'operation' string input argument.

        In general, try to avoid this method and use 'queue_job', 'get_job' and 'retrieve_job_result' manually instead,
        since this is a blocking operation.

        If the 'operation' is a JobDesc instance, and that JobDesc instance has the 'save' attribute
        set, it will be ignored, so you'll have to store any results manually.

        Arguments:
            operation: a module name, operation id, or a path to a pipeline file (resolved in this order, until a match is found)..
            inputs: the operation inputs
            comment: a (required) comment to attach to the job
            operation_config: the (optional) module config in case 'operation' is a module name

        Returns:
            the job result value map

        """

        if not comment and comment != "":
            from kiara.exceptions import KiaraException

            raise KiaraException(msg="Can't submit job: no comment provided.")

        if inputs is None:
            inputs = {}

        return self._api.run_job(
            operation=operation,
            inputs=inputs,
            operation_config=operation_config,
            comment=comment,
        )

    def queue_job(
        self,
        operation: Union[str, Path, "Manifest", "OperationInfo", "JobDesc"],
        inputs: Mapping[str, Any],
        comment: str,
        operation_config: Union[None, Mapping[str, Any]] = None,
    ) -> uuid.UUID:
        """
        Queue a job from a operation id, module_name (and config), or pipeline file, wait for the job to finish and retrieve the result.

        This is a convenience method that auto-detects what is meant by the 'operation' string input argument.

        If the 'operation' is a JobDesc instance, and that JobDesc instance has the 'save' attribute
        set, it will be ignored, so you'll have to store any results manually.

        Arguments:
            operation: a module name, operation id, or a path to a pipeline file (resolved in this order, until a match is found)..
            inputs: the operation inputs
            comment: a (required) comment to attach to the job
            operation_config: the (optional) module config in case 'operation' is a module name

        Returns:
            the queued job id
        """

        if not comment and comment != "":
            from kiara.exceptions import KiaraException

            raise KiaraException(msg="Can't submit job: no comment provided.")

        return self._api.queue_job(
            operation=operation,
            inputs=inputs,
            operation_config=operation_config,
            comment=comment,
        )

    def set_job_comment(
        self, job_id: Union[str, uuid.UUID], comment: str, force: bool = True
    ):
        """Set a comment for the specified job.

        Arguments:
            job_id: the job id
            comment: the comment to set
            force: whether to overwrite an existing comment
        """

        from kiara.models.metadata import CommentMetadata

        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)

        comment_metadata = CommentMetadata(comment=comment)
        items = {"comment": comment_metadata}

        self._api.context.metadata_registry.register_job_metadata_items(
            job_id=job_id, items=items
        )

    def get_job_comment(self, job_id: Union[str, uuid.UUID]) -> Union[str, None]:
        """Retrieve the comment for the specified job.

        Returns 'None' if the job_id does not exist, or the job does not have a comment attached to it.

        Arguments:
            job_id: the job id

        Returns:
            the comment as string, or None
        """

        from kiara.models.metadata import CommentMetadata

        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)

        metadata: Union[None, "KiaraMetadata"] = (
            self._api.context.metadata_registry.retrieve_job_metadata_item(
                job_id=job_id, key="comment"
            )
        )

        if not metadata:
            return None

        if not isinstance(metadata, CommentMetadata):
            from kiara.exceptions import KiaraException

            raise KiaraException(
                msg=f"Metadata item 'comment' for job '{job_id}' is not a comment."
            )
        return metadata.comment

    # BEGIN IMPORTED-ENDPOINTS
    def list_available_plugin_names(
        self, regex: str = r"^kiara[-_]plugin\..*"
    ) -> List[str]:
        r"""Get a list of all available plugins.

        Arguments:
            regex: an optional regex to indicate the plugin naming scheme (default: /$kiara[_-]plugin\..*/)

        Returns:
            a list of plugin names
        """

        result: List[str] = self._api.list_available_plugin_names(regex=regex)
        return result

    def retrieve_plugin_info(self, plugin_name: str) -> "KiaraPluginInfo":
        """Get information about a plugin.

        This contains information about included data-types, modules, operations, pipelines, as well as metadata
        about author(s), etc.

        Arguments:
            plugin_name: the name of the plugin

        Returns:
            a dictionary with information about the plugin
        """

        result: "KiaraPluginInfo" = self._api.retrieve_plugin_info(
            plugin_name=plugin_name
        )
        return result

    def retrieve_plugin_infos(
        self, plugin_name_regex: str = r"^kiara[-_]plugin\..*"
    ) -> "KiaraPluginInfos":
        """Get information about multiple plugins.

        This is just a convenience method to get information about multiple plugins at once.
        """

        result: "KiaraPluginInfos" = self._api.retrieve_plugin_infos(
            plugin_name_regex=plugin_name_regex
        )
        return result

    def get_context_info(self) -> "ContextInfo":
        """Retrieve information about the current kiara context.

        This contains information about the context, like its name/alias, the values & aliases it contains, and which archives are connected to it.
        """

        result: "ContextInfo" = self._api.get_context_info()
        return result

    def list_context_names(self) -> List[str]:
        """list the names of all available/registered contexts.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and
        whether we want to support single-file contexts in the future.
        """

        result: List[str] = self._api.list_context_names()
        return result

    def retrieve_context_infos(self) -> "ContextInfos":
        """Retrieve information about the available/registered contexts.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and whether we want to support single-file contexts in the future.
        """

        result: "ContextInfos" = self._api.retrieve_context_infos()
        return result

    def get_current_context_name(self) -> str:
        """Retrieve the name of the current context.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and whether we want to support single-file contexts in the future.
        """

        result: str = self._api.get_current_context_name()
        return result

    def set_active_context(self, context_name: str, create: bool = False):
        """Set the currently active context for this KiarAPI instance.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and whether we want to support single-file contexts in the future.
        """

        self._api.set_active_context(context_name=context_name, create=create)

    def list_data_type_names(self, include_profiles: bool = False) -> List[str]:
        """Get a list of all registered data types.

        Arguments:
            include_profiles: if True, also include the names of all registered data type profiles
        """

        result: List[str] = self._api.list_data_type_names(
            include_profiles=include_profiles
        )
        return result

    def retrieve_data_types_info(
        self,
        filter: Union[str, Iterable[str], None] = None,
        include_data_type_profiles: bool = False,
        python_package: Union[None, str] = None,
    ) -> "DataTypeClassesInfo":
        """Retrieve information about all data types.

        A data type is a Python class that inherits from [DataType[kiara.data_types.DataType], and it wraps a specific
        Python class that holds the actual data and provides metadata and convenience methods for managing the data internally. Data types are not directly used by users, but they are exposed in the input/output schemas of moudles and other data-related features.

        Arguments:
            filter: an optional string or (list of strings) the returned datatype ids have to match (all filters in the case of a list)
            include_data_type_profiles: if True, also include the names of all registered data type profiles
            python_package: if provided, only return data types that are defined in the given python package

        Returns:
            an object containing all information about all data types
        """

        result: "DataTypeClassesInfo" = self._api.retrieve_data_types_info(
            filter=filter,
            include_data_type_profiles=include_data_type_profiles,
            python_package=python_package,
        )
        return result

    def retrieve_data_type_info(self, data_type_name: str) -> "DataTypeClassInfo":
        """Retrieve information about a specific data type.

        Arguments:
            data_type: the registered name of the data type

        Returns:
            an object containing all information about a data type
        """

        result: "DataTypeClassInfo" = self._api.retrieve_data_type_info(
            data_type_name=data_type_name
        )
        return result

    def list_module_type_names(self) -> List[str]:
        """Get a list of all registered module types."""

        result: List[str] = self._api.list_module_type_names()
        return result

    def retrieve_module_types_info(
        self,
        filter: Union[None, str, Iterable[str]] = None,
        python_package: Union[str, None] = None,
    ) -> "ModuleTypesInfo":
        """Retrieve information for all available module types (or a filtered subset thereof).

        A module type is Python class that inherits from [KiaraModule][kiara.modules.KiaraModule], and is the basic
        building block for processing pipelines. Module types are not used directly by users, Operations are. Operations
         are instantiated modules (meaning: the module & some (optional) configuration).

        Arguments:
            filter: an optional string (or list of string) the returned module names have to match (all filters in case of list)
            python_package: an optional string, if provided, only modules from the specified python package are returned

        Returns:
            a mapping object containing module names as keys, and information about the modules as values
        """

        result: "ModuleTypesInfo" = self._api.retrieve_module_types_info(
            filter=filter, python_package=python_package
        )
        return result

    def retrieve_module_type_info(self, module_type: str) -> "ModuleTypeInfo":
        """Retrieve information about a specific module type.

        This can be used to retrieve information like module documentation and configuration options.

        Arguments:
            module_type: the registered name of the module

        Returns:
            an object containing all information about a module type
        """

        result: "ModuleTypeInfo" = self._api.retrieve_module_type_info(
            module_type=module_type
        )
        return result

    def list_operation_ids(
        self,
        filter: Union[str, None, Iterable[str]] = None,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        operation_types: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
        python_packages: Union[str, None, Iterable[str]] = None,
    ) -> List[str]:
        """Get a list of all operation ids that match the specified filter.

        Arguments:
            filter: the (optional) filter string(s), an operation must match all of them to be included in the result
            input_types: each operation must have at least one input that matches one of the specified types
            output_types: each operation must have at least one output that matches one of the specified types
            operation_types: only include operations of the specified type(s)
            include_internal: whether to include operations that are predominantly used internally in kiara.
            python_packages: only include operations that are contained in one of the provided python packages
        """

        result: List[str] = self._api.list_operation_ids(
            filter=filter,
            input_types=input_types,
            output_types=output_types,
            operation_types=operation_types,
            include_internal=include_internal,
            python_packages=python_packages,
        )
        return result

    def get_operation(
        self,
        operation: Union[Mapping[str, Any], str, "Path"],
        allow_external: Union[bool, None] = None,
    ) -> "Operation":
        """Return the operation instance with the specified id.

        The difference to the 'create_operation' endpoint is slight, in most cases you could use either of them, but this one is a bit more convenient in most cases, as it tries to do the right thing with whatever 'operation' argument you use it. The 'create_opearation' endpoint will always create a new 'Operation' instance, while this may or may not return a re-used one.

        This endpoint can be used to get information about a specific operation, like inputs/outputs scheman, documentation, etc.

        The order in which the operation argument is resolved:
        - if it's a string, and an existing, registered operation_id, the associated operation is returned
        - if it's a path to an existing file, the content of the file is loaded into a dict and depending on the content a pipeline module will be created, or a 'normal' manifest (if module_type is a key in the dict)

        Arguments:
            operation: the operation id, module_type_name, path to a file, or url
            allow_external: if True, allow loading operations from external sources (e.g. a URL), if 'None' is provided, the configured value in the runtime configuration is used.

        Returns:
            operation instance data
        """

        result: "Operation" = self._api.get_operation(
            operation=operation, allow_external=allow_external
        )
        return result

    def list_operations(
        self,
        filter: Union[str, None, Iterable[str]] = None,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        operation_types: Union[str, Iterable[str], None] = None,
        python_packages: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
    ) -> "OperationsMap":
        """List all available operations, optionally filter.

        Arguments:
            filter: the (optional) filter string(s), an operation must match all of them to be included in the result
            input_types: each operation must have at least one input that matches one of the specified types
            output_types: each operation must have at least one output that matches one of the specified types
            operation_types: only include operations of the specified type(s)
            include_internal: whether to include operations that are predominantly used internally in kiara.
            python_packages: only include operations that are contained in one of the provided python packages

        Returns:
            a dictionary with the operation id as key, and [kiara.models.module.operation.Operation] instance data as value
        """

        result: "OperationsMap" = self._api.list_operations(
            filter=filter,
            input_types=input_types,
            output_types=output_types,
            operation_types=operation_types,
            python_packages=python_packages,
            include_internal=include_internal,
        )
        return result

    def retrieve_operation_info(
        self, operation: str, allow_external: bool = False
    ) -> "OperationInfo":
        """Return the full information for the specified operation id.

        This is similar to the 'get_operation' method, but returns additional information. Only use this instead of
        'get_operation' if you need the additional info, as it's more expensive to get.

        Arguments:
            operation: the operation id

        Returns:
            augmented operation instance data
        """

        result: "OperationInfo" = self._api.retrieve_operation_info(
            operation=operation, allow_external=allow_external
        )
        return result

    def retrieve_operations_info(
        self,
        *filters: str,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        operation_types: Union[str, Iterable[str], None] = None,
        python_packages: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
    ) -> "OperationGroupInfo":
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
            input_types: each operation must have at least one input that matches one of the specified types
            output_types: each operation must have at least one output that matches one of the specified types
            operation_types: only include operations of the specified type(s)
            include_internal: whether to include operations that are predominantly used internally in kiara.
            python_packages: only include operations that are contained in one of the provided python packages
        Returns:
            a wrapper object containing a dictionary of items with value_id as key, and [kiara.interfaces.python_api.models.info.OperationInfo] as value
        """

        result: "OperationGroupInfo" = self._api.retrieve_operations_info(
            *filters,
            input_types=input_types,
            output_types=output_types,
            operation_types=operation_types,
            python_packages=python_packages,
            include_internal=include_internal,
        )
        return result

    def list_all_value_ids(self) -> List["UUID"]:
        """List all value ids in the current context.

        This returns everything, even internal values. It should be faster than using
        `list_value_ids` with equivalent parameters, because no filtering has to happen.

        Returns:
            all value_ids in the current context, using every registered store
        """

        result: List["UUID"] = self._api.list_all_value_ids()
        return result

    def list_value_ids(self, **matcher_params: Any) -> List["UUID"]:
        """List all available value ids for this kiara context.

        By default, this also includes internal values.

        This method exists mainly so frontends can retrieve a list of all value_ids that exists on the backend without
        having to look up the details of each value (like [list_values][kiara.interfaces.python_api.KiaraAPI.list_values]
        does). This method can also be used with a matcher, but in this case the [list_values][kiara.interfaces.python_api.KiaraAPI.list_values]
        would be preferable in most cases, because it is called under the hood, and the performance advantage of not
        having to look up value details is gone.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters and defaults

        Returns:
            a list of value ids
        """

        result: List["UUID"] = self._api.list_value_ids(**matcher_params)
        return result

    def list_all_values(self) -> "ValueMapReadOnly":
        """List all values in the current context, incl. internal ones.

        This should be faster than `list_values` with equivalent matcher params, because no
        filtering has to happen.
        """

        result: "ValueMapReadOnly" = self._api.list_all_values()
        return result

    def list_values(self, **matcher_params: Any) -> "ValueMapReadOnly":
        """List all available (relevant) values, optionally filter.

        Retrieve information about all values that are available in the current kiara context session (both stored and non-stored).

        Check the `ValueMatcher` class for available parameters and defaults, for example this excludes
        internal values by default.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a dictionary with value_id as key, and [kiara.models.values.value.Value] as value
        """

        result: "ValueMapReadOnly" = self._api.list_values(**matcher_params)
        return result

    def get_value(self, value: Union[str, "Value", "UUID", "Path"]) -> "Value":
        """Retrieve a value instance with the specified id or alias.

        Basically a convenience method to convert any possible Python type into
        a 'Value' instance. Raises an exception if no value could be found.

        Arguments:
            value: a value id, alias or object that has a 'value_id' attribute.

        Returns:
            the Value instance
        """

        result: "Value" = self._api.get_value(value=value)
        return result

    def get_values(self, **values: Union[str, "Value", "UUID"]) -> "ValueMapReadOnly":
        """Retrieve Value instances for the specified value ids or aliases.

        This is a convenience method to get fully 'hydrated' `Value` objects from references to them.

        Arguments:
            values: a dictionary with value ids or aliases as keys, and value instances as values

        Returns:
            a mapping with value_id as key, and [kiara.models.values.value.Value] as value
        """

        result: "ValueMapReadOnly" = self._api.get_values(**values)
        return result

    def retrieve_value_info(
        self, value: Union[str, "UUID", "Value", "Path"]
    ) -> "ValueInfo":
        """Retrieve an info object for a value.

        Companion method to 'get_value', 'ValueInfo' objects contains augmented information on top of what 'normal' [Value][kiara.models.values.value.Value] objects
        hold (like resolved properties for example), but they can take longer to create/resolve. If you don't need any
        of the augmented information, just use the [get_value][kiara.interfaces.python_api.KiaraAPI.get_value] method
        instead.

        Arguments:
            value: a value id, alias or object that has a 'value_id' attribute.

        Returns:
            the ValueInfo instance
        """

        result: "ValueInfo" = self._api.retrieve_value_info(value=value)
        return result

    def retrieve_values_info(self, **matcher_params: Any) -> "ValuesInfo":
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

        result: "ValuesInfo" = self._api.retrieve_values_info(**matcher_params)
        return result

    def list_alias_names(self, **matcher_params: Any) -> List[str]:
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

        result: List[str] = self._api.list_alias_names(**matcher_params)
        return result

    def list_aliases(self, **matcher_params: Any) -> "ValueMapReadOnly":
        """List all available values that have an alias assigned, optionally filter.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a dictionary with value_id as key, and [kiara.models.values.value.Value] as value
        """

        result: "ValueMapReadOnly" = self._api.list_aliases(**matcher_params)
        return result

    def retrieve_aliases_info(self, **matcher_params: Any) -> "ValuesInfo":
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

        result: "ValuesInfo" = self._api.retrieve_aliases_info(**matcher_params)
        return result

    def store_value(
        self,
        value: Union[str, "UUID", "Value"],
        alias: Union[str, Iterable[str], None],
        allow_overwrite: bool = True,
        store: Union[str, None] = None,
        store_related_metadata: bool = True,
        set_as_store_default: bool = False,
    ) -> "StoreValueResult":
        """Store the specified value in a value store.

        If you provide values for the 'data_store' and/or 'alias_store' other than 'default', you need
        to make sure those stores are registered with the current context. In most cases, the 'export' endpoint (to be done) will probably be an easier way to export values, which I suspect will
        be the main use-case for this endpoint if any of the 'store' arguments where needed. Otherwise, this endpoint is useful to persist values for use in later seperate sessions.

        This method does not raise an error if the storing of the value fails, so you have to investigate the
        'StoreValueResult' instance that is returned to see if the storing was successful.

        Arguments:
            value: the value (or a reference to it)
            alias: (Optional) one or several aliases for the value
            allow_overwrite: whether to allow overwriting existing aliases
            store: in case data and alias store names are the same, you can use this, if you specify one or both of the others, this will be overwritten
            store_related_metadata: whether to store related metadata (comments, etc.) in the same store as the data
            set_as_store_default: whether to set the specified store as the default store for the value
        """

        result: "StoreValueResult" = self._api.store_value(
            value=value,
            alias=alias,
            allow_overwrite=allow_overwrite,
            store=store,
            store_related_metadata=store_related_metadata,
            set_as_store_default=set_as_store_default,
        )
        return result

    def store_values(
        self,
        values: Union[
            str,
            "Value",
            "UUID",
            Mapping[str, Union[str, "UUID", "Value"]],
            Iterable[Union[str, "UUID", "Value"]],
        ],
        alias_map: Union[Mapping[str, Iterable[str]], bool, str] = False,
        allow_alias_overwrite: bool = True,
        store: Union[str, None] = None,
        store_related_metadata: bool = True,
    ) -> "StoreValuesResult":
        """Store multiple values into the (default) kiara value store.

        Convenience method to store multiple values. In a lot of cases you can be more flexible if you
        loop over the values on the frontend side, and call the 'store_value' method for each value. But this might be meaningfully slower. This method has the potential to be optimized in the future.

        You have several options to provide the values and aliases you want to store:

        - as a string, in which case the item will be wrapped in a list (see non-mapping iterable below)

        - as a (non-mapping) iterable of value items, those can either be:

          - a value id (as string or uuid)
          - a value alias (as string)
          - a value instance

        If you do that, then the 'alias_map' argument can either be:

          - 'False', in which case no aliases will be registered
          - 'True', in which case all items in the 'values' iterable must be a valid alias, and the alias will be copied without change to the new store
          - a 'string', in which case all items in the 'values' iterable also must be a valid alias, and the alias that will be registered in the new store will use the string value as prefix (e.g. 'alias_map' = 'experiment1' and 'values' = ['a', 'b'] will result in the aliases 'experiment1.a' and 'experiment1.b')
          - a map that uses the stringi-fied uuid of the value that should get one or several aliases as key, and a list of aliases as values

        You can also use a mapping type (like a dict) for the 'values' argument. In this case, the key is a string, and the value can be:

          - a value id (as string or uuid)
          - a value alias (as string)
          - a value instance

        In this case, the meaning of the 'alias_map' is as follows:

          - 'False': no aliases will be registered
          - 'True': the key in the 'values' argument will be used as alias
          - a string: all keys from the 'values' map will be used as alias, prefixed with the value of 'alias_map'
          - another map, with a string referring to the key in the 'values' argument as key, and a list of aliases (strings) as value

        Sorry, this is all a bit convoluted, but it's the only way I could think of to make this work for all the requirements I had. In most keases, you'll only have to use 'True' or 'False' here, hopefully.

        This method does not raise an error if the storing of the value fails, so you have to investigate the
        'StoreValuesResult' instance that is returned to see if the storing was successful.

        Arguments:
            values: an iterable/map of value keys/values
            alias_map: a map of value keys aliases
            allow_alias_overwrite: whether to allow overwriting existing aliases
            store: in case data and alias store names are the same, you can use this, if you specify one or both of the others, this will be overwritten
            data_store: the registered name (or archive id as string) of the store to write the data
            alias_store: the registered name (or archive id as string) of the store to persist the alias(es)/value_id mapping

        Returns:
            an object outlining which values (identified by the specified value key or an enumerated index) where stored and how
        """

        result: "StoreValuesResult" = self._api.store_values(
            values=values,
            alias_map=alias_map,
            allow_alias_overwrite=allow_alias_overwrite,
            store=store,
            store_related_metadata=store_related_metadata,
        )
        return result

    def import_values(
        self,
        source_archive: Union[str, "Path"],
        values: Union[
            str,
            Mapping[str, Union[str, "UUID", "Value"]],
            Iterable[Union[str, "UUID", "Value"]],
        ],
        alias_map: Union[Mapping[str, Iterable[str]], bool, str] = False,
        allow_alias_overwrite: bool = True,
        source_registered_name: Union[str, None] = None,
    ) -> "StoreValuesResult":
        """Import one or several values from an external kiara archive, along with their aliases (optional).

        For the 'values' & 'alias_map' arguments, see the 'store_values' endpoint, as they will be forwarded to that endpoint as is,
        and there are several ways to use them which is information I don't want to duplicate.

        If you provide aliases in the 'values' parameter, the aliases must be available in the external archive.

        Currently, this only works with an external archive file, not with an archive that is registered into the context.
        This will probably be added later on, let me know if there is demand, then I'll prioritize.

        This method does not raise an error if the storing of the value fails, so you have to investigate the
        'StoreValuesResult' instance that is returned to see if the storing was successful.

        # NOTE: this is a preliminary endpoint, and might be changed in the future. If you have a use-case for this, please let me know.

        Arguments:
            source_archive: the name of the archive to store the values into
            values: an iterable/map of value keys/values
            alias_map: a map of value keys aliases
            allow_alias_overwrite: whether to allow overwriting existing aliases
            source_registered_name: the name to register the archive under in the context
        """

        result: "StoreValuesResult" = self._api.import_values(
            source_archive=source_archive,
            values=values,
            alias_map=alias_map,
            allow_alias_overwrite=allow_alias_overwrite,
            source_registered_name=source_registered_name,
        )
        return result

    def export_values(
        self,
        target_archive: Union[str, "Path"],
        values: Union[
            str,
            Mapping[str, Union[str, "UUID", "Value"]],
            Iterable[Union[str, "UUID", "Value"]],
        ],
        alias_map: Union[Mapping[str, Iterable[str]], bool, str] = False,
        allow_alias_overwrite: bool = True,
        target_registered_name: Union[str, None] = None,
        append: bool = False,
        target_store_params: Union[None, Mapping[str, Any]] = None,
        export_related_metadata: bool = True,
        additional_archive_metadata: Union[None, Mapping[str, Any]] = None,
    ) -> "StoreValuesResult":
        """Store one or several values along with (optional) aliases into a kiara archive.

        For the 'values' & 'alias_map' arguments, see the 'store_values' endpoint, as they will be forwarded to that endpoint as is,
        and there are several ways to use them which is information I don't want to duplicate.

        Currently, this only works with an external archive file, not with an archive that is registered into the context.
        This will probably be added later on, let me know if there is demand, then I'll prioritize.

        'target_store_params' is used if the archive does not exist yet. The one supported value for the 'target_store_params' argument currently is 'compression', which can be one of:

        - zstd: zstd compression (default) -- fairly fast, and good compression
        - none: no compression
        - LZMA: LZMA compression -- very slow, but very good compression
        - LZ4: LZ4 compression -- very fast, but not as good compression as zstd

        This method does not raise an error if the storing of the value fails, so you have to investigate the
        'StoreValuesResult' instance that is returned to see if the storing was successful.

        # NOTE: this is a preliminary endpoint, and might be changed in the future. If you have a use-case for this, please let me know.

        Arguments:
            target_store: the name of the archive to store the values into
            values: an iterable/map of value keys/values
            alias_map: a map of value keys aliases
            allow_alias_overwrite: whether to allow overwriting existing aliases
            target_registered_name: the name to register the archive under in the context
            append: whether to append to an existing archive
            target_store_params: additional parameters to pass to the 'create_kiarchive' method if the file does not exist yet
            export_related_metadata: whether to export related metadata (e.g. job info, comments, ..) to the new archive or not
            additional_archive_metadata: (optional) additional metadata to add to the archive
        """

        result: "StoreValuesResult" = self._api.export_values(
            target_archive=target_archive,
            values=values,
            alias_map=alias_map,
            allow_alias_overwrite=allow_alias_overwrite,
            target_registered_name=target_registered_name,
            append=append,
            target_store_params=target_store_params,
            export_related_metadata=export_related_metadata,
            additional_archive_metadata=additional_archive_metadata,
        )
        return result

    def retrieve_archive_info(
        self, archive: Union[str, "KiArchive"]
    ) -> "KiArchiveInfo":
        """Retrieve information about an archive at the specified local path

        Currently, this only works with an external archive file, not with an archive that is registered into the context.
        This will probably be added later on, let me know if there is demand, then I'll prioritize.

        # NOTE: this is a preliminary endpoint, and might be changed in the future. If you have a use-case for this, please let me know.

        Arguments:
            archive: the uri of the archive (file path)

        Returns:
            a [KiarchiveInfo][kiara.interfaces.python_api.models.archive.KiarchiveInfo] instance, containing details about the archive
        """

        result: "KiArchiveInfo" = self._api.retrieve_archive_info(archive=archive)
        return result

    def export_archive(
        self,
        target_archive: Union[str, "Path"],
        target_registered_name: Union[str, None] = None,
        append: bool = False,
        no_aliases: bool = False,
        target_store_params: Union[None, Mapping[str, Any]] = None,
    ) -> "StoreValuesResult":
        """Export all data from the default store in your context into the specfied archive path.

        The target archives will be registered into the context, either using the provided registered_name, or the name
        will be auto-determined from the archive metadata.

        Currently, this only works with an external archive file, not with an archive that is already registered into the context.
        This will be added later on.

        Also, currently you can only export all data from the default store, there is no way to select only a sub-set. This will
        also be supported later on.

        The one supported value for the 'target_store_params' argument currently is 'compression', which can be one of:

        - zstd: zstd compression (default) -- fairly fast, and good compression
        - none: no compression
        - LZMA: LZMA compression -- very slow, but very good compression
        - LZ4: LZ4 compression -- very fast, but not as good compression as zstd

        This method does not raise an error if the storing of the value fails, so you have to investigate the
        'StoreValuesResult' instance that is returned to see if the storing was successful

        Arguments:
            target_archive: the registered_name or uri of the target archive
            target_registered_name: the name/alias that the archive should be registered in the context (if necessary)
            append: whether to append to an existing archive or error out if the target already exists
            no_aliases: whether to skip importing aliases
            target_store_params: additional parameters to pass to the 'create_kiarchive' method if the target file does not exist yet

        Returns:
            an object outlining which values (identified by the specified value key or an enumerated index) where stored and how
        """

        result: "StoreValuesResult" = self._api.export_archive(
            target_archive=target_archive,
            target_registered_name=target_registered_name,
            append=append,
            no_aliases=no_aliases,
            target_store_params=target_store_params,
        )
        return result

    def import_archive(
        self,
        source_archive: Union[str, "Path"],
        source_registered_name: Union[str, None] = None,
        no_aliases: bool = False,
    ) -> "StoreValuesResult":
        """Import all data from the specified archive into the current contexts default data & alias store.

        The source target will be registered into the context, either using the provided registered_name, otherwise the name
        will be auto-determined from the archive metadata.

        Currently, this only works with an external archive file, not with an archive that is registered into the context.
        This will be added later on.

        Also, currently you can only import all data into the default store, there is no way to select only a sub-set. This will
        also be supported later on.

        This method does not raise an error if the storing of the value fails, so you have to investigate the
        'StoreValuesResult' instance that is returned to see if the storing was successful

        Arguments:
            source_archive: the registered_name or uri of the source archive
            source_registered_name: the name/alias that the archive should be registered in the context (if necessary)
            no_aliases: whether to skip importing aliases

        Returns:
            an object outlining which values (identified by the specified value key or an enumerated index) where stored and how
        """

        result: "StoreValuesResult" = self._api.import_archive(
            source_archive=source_archive,
            source_registered_name=source_registered_name,
            no_aliases=no_aliases,
        )
        return result

    def get_job(self, job_id: Union[str, "UUID"]) -> "ActiveJob":
        """Retrieve the status of the job with the provided id."""

        result: "ActiveJob" = self._api.get_job(job_id=job_id)
        return result

    def get_job_result(self, job_id: Union[str, "UUID"]) -> "ValueMapReadOnly":
        """Retrieve the result(s) of the specified job."""

        result: "ValueMapReadOnly" = self._api.get_job_result(job_id=job_id)
        return result

    def list_all_job_record_ids(self) -> List["UUID"]:
        """List all available job ids in this kiara context, ordered from newest to oldest, including internal jobs.

        This should be faster than `list_job_record_ids` with equivalent parameters, because no filtering
        needs to be done.
        """

        result: List["UUID"] = self._api.list_all_job_record_ids()
        return result

    def list_job_record_ids(self, **matcher_params: Any) -> List["UUID"]:
        """List all available job ids in this kiara context, ordered from newest to oldest.

        You can look up the supported matcher parameter arguments via the [JobMatcher][kiara.models.module.jobs.JobMatcher] class. By default, this method for example
        does not return jobs marked as 'internal'.

        Arguments:
            matcher_params: additional parameters to pass to the job matcher

        Returns:
            a list of job ids, ordered from latest to earliest
        """

        result: List["UUID"] = self._api.list_job_record_ids(**matcher_params)
        return result

    def list_all_job_records(self) -> Mapping["UUID", "JobRecord"]:
        """List all available job records in this kiara context, ordered from newest to oldest, including internal jobs.

        This should be faster than `list_job_records` with equivalent parameters, because no filtering
        needs to be done.
        """

        result: Mapping["UUID", "JobRecord"] = self._api.list_all_job_records()
        return result

    def list_job_records(self, **matcher_params: Any) -> Mapping["UUID", "JobRecord"]:
        """List all available job ids in this kiara context, ordered from newest to oldest.

        You can look up the supported matcher parameter arguments via the [JobMatcher][kiara.models.module.jobs.JobMatcher] class. By default, this method for example
        does not return jobs marked as 'internal'.

        You can look up the supported matcher parameter arguments via the [JobMatcher][kiara.models.module.jobs.JobMatcher] class.

        Arguments:
            matcher_params: additional parameters to pass to the job matcher

        Returns:
            a list of job details, ordered from latest to earliest
        """

        result: Mapping["UUID", "JobRecord"] = self._api.list_job_records(
            **matcher_params
        )
        return result

    def get_job_record(self, job_id: Union[str, "UUID"]) -> Union["JobRecord", None]:
        """Retrieve the detailed job record for the specified job id.

        If no job can be found, 'None' is returned.
        """

        result: Union["JobRecord", None] = self._api.get_job_record(job_id=job_id)
        return result

    # END IMPORTED-ENDPOINTS
