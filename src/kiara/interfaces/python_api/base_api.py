# -*- coding: utf-8 -*-
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import inspect
import json
import os.path
import sys
import textwrap
import uuid
from functools import cached_property
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Set,
    Type,
    Union,
)

import dpath
import structlog
from ruamel.yaml import YAML

from kiara.defaults import (
    CHUNK_COMPRESSION_TYPE,
    DATA_ARCHIVE_DEFAULT_VALUE_MARKER,
    DEFAULT_STORE_MARKER,
    OFFICIAL_KIARA_PLUGINS,
    VALID_VALUE_QUERY_CATEGORIES,
    VALUE_ATTR_DELIMITER,
)
from kiara.exceptions import (
    DataTypeUnknownException,
    KiaraException,
    NoSuchExecutionTargetException,
    NoSuchWorkflowException,
)
from kiara.interfaces.python_api.models.info import (
    DataTypeClassesInfo,
    DataTypeClassInfo,
    KiaraPluginInfo,
    KiaraPluginInfos,
    ModuleTypeInfo,
    ModuleTypesInfo,
    OperationGroupInfo,
    OperationInfo,
    OperationTypeInfo,
    RendererInfos,
    ValueInfo,
    ValuesInfo,
)
from kiara.interfaces.python_api.models.job import JobDesc
from kiara.interfaces.python_api.value import StoreValueResult, StoreValuesResult
from kiara.models.context import ContextInfo, ContextInfos
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import Operation
from kiara.models.rendering import RenderValueResult
from kiara.models.runtime_environment.python import PythonRuntimeEnvironment
from kiara.models.values.matchers import ValueMatcher
from kiara.models.values.value import (
    PersistedData,
    Value,
    ValueMapReadOnly,
    ValueSchema,
)
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
from kiara.registries.environment import EnvironmentRegistry
from kiara.registries.ids import ID_REGISTRY
from kiara.renderers import KiaraRenderer
from kiara.utils import log_exception, log_message
from kiara.utils.downloads import get_data_from_url
from kiara.utils.files import get_data_from_file
from kiara.utils.operations import create_operation
from kiara.utils.string_vars import replace_var_names_in_obj

if TYPE_CHECKING:
    from kiara.context import Kiara, KiaraConfig, KiaraRuntimeConfig
    from kiara.interfaces.python_api.models.archive import KiArchive
    from kiara.interfaces.python_api.models.doc import (
        OperationsMap,
        PipelinesMap,
        WorkflowsMap,
    )
    from kiara.interfaces.python_api.workflow import Workflow
    from kiara.models.archives import KiArchiveInfo
    from kiara.models.module.jobs import ActiveJob, JobRecord
    from kiara.models.module.pipeline import PipelineConfig, PipelineStructure
    from kiara.models.module.pipeline.pipeline import PipelineGroupInfo, PipelineInfo
    from kiara.registries import KiaraArchive
    from kiara.registries.metadata import MetadataStore

logger = structlog.getLogger()
yaml = YAML(typ="safe")


def tag(*tags: str):
    def decorator(func):
        func._tags = tags
        return func

    return decorator


def find_base_api_endpoints(cls, label):
    """Return all endpoints that are tagged with the provided label."""

    # for func in dir(cls):
    #     if not func.startswith("_") and "_tags" not in dir(getattr(cls, func)):
    #         print(dir(getattr(cls, func)))
    return [
        getattr(cls, func)
        for func in dir(cls)
        if "_tags" in dir(getattr(cls, func)) and label in getattr(cls, func)._tags
    ]


class BaseAPI(object):
    """Kiara base API.

    This class wraps a [Kiara][kiara.context.kiara.Kiara] instance, and allows easy a access to tasks that are
    typically done by a frontend. The return types of each method are json seriable in most cases.

    Can be extended for special scenarios and augmented with scenario-specific methdos (Jupyter, web-frontend, ...)

    The naming of the API endpoints follows a (loose-ish) convention:
    - list_*: return a list of ids or items, if items, filtering is supported
    - get_*: get specific instances of a type (operation, value, etc.)
    - retrieve_*: get augmented information about an instance or type of something. This usually implies that there is some overhead,
    so before you use this, make sure that there is not 'get_*' or 'list_*' endpoint that could give you what you need.
    .
    """

    def __init__(self, kiara_config: Union["KiaraConfig", None] = None):

        if kiara_config is None:
            from kiara.context import Kiara, KiaraConfig

            kiara_config = KiaraConfig()

        self._kiara_config: KiaraConfig = kiara_config
        self._contexts: Dict[str, Kiara] = {}
        self._workflow_cache: Dict[uuid.UUID, Workflow] = {}

        self._current_context: Union[None, Kiara] = None
        self._current_context_alias: Union[None, str] = None

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

    @tag("kiara_api")
    def list_available_plugin_names(
        self, regex: str = "^kiara[-_]plugin\\..*"
    ) -> List[str]:
        r"""
        Get a list of all available plugins.

        Arguments:
            regex: an optional regex to indicate the plugin naming scheme (default: /$kiara[_-]plugin\..*/)

        Returns:
            a list of plugin names
        """

        if not regex:
            regex = "^kiara[-_]plugin\\..*"

        return KiaraPluginInfos.get_available_plugin_names(
            kiara=self.context, regex=regex
        )

    @tag("kiara_api")
    def retrieve_plugin_info(self, plugin_name: str) -> KiaraPluginInfo:
        """
        Get information about a plugin.

        This contains information about included data-types, modules, operations, pipelines, as well as metadata
        about author(s), etc.

        Arguments:
            plugin_name: the name of the plugin

        Returns:
            a dictionary with information about the plugin
        """

        info = KiaraPluginInfo.create_from_instance(
            kiara=self.context, instance=plugin_name
        )
        return info

    @tag("kiara_api")
    def retrieve_plugin_infos(
        self, plugin_name_regex: str = "^kiara[-_]plugin\\..*"
    ) -> KiaraPluginInfos:
        """Get information about multiple plugins.

        This is just a convenience method to get information about multiple plugins at once.
        """

        if not plugin_name_regex:
            plugin_name_regex = "^kiara[-_]plugin\\..*"

        plugin_infos = KiaraPluginInfos.create_group(
            self.context, None, plugin_name_regex
        )
        return plugin_infos

    @property
    def context(self) -> "Kiara":
        """
        Return the kiara context.

        DON"T USE THIS! This is going away in the production release.
        """
        if self._current_context is None:
            self._current_context = self._kiara_config.create_context(
                extra_pipelines=None
            )
            self._current_context_alias = self._kiara_config.default_context

        return self._current_context

    def get_runtime_config(self) -> "KiaraRuntimeConfig":
        """Retrieve the current runtime configuration.

        Check the 'KiaraRuntimeConfig' class for more information about the available options.
        """
        return self.context.runtime_config

    @tag("kiara_api")
    def get_context_info(self) -> ContextInfo:
        """Retrieve information about the current kiara context.

        This contains information about the context, like its name/alias, the values & aliases it contains, and which archives are connected to it.

        """
        context_config = self._kiara_config.get_context_config(
            self.get_current_context_name()
        )
        info = ContextInfo.create_from_context_config(
            context_config,
            context_name=self.get_current_context_name(),
            runtime_config=self._kiara_config.runtime_config,
        )

        return info

    def ensure_plugin_packages(
        self, package_names: Union[str, Iterable[str]], update: bool = False
    ) -> Union[bool, None]:
        """
        Ensure that the specified packages are installed.


        NOTE: this is not tested, and it might go away in the future, so don't rely on it being available long-term. Ideally, we'll have other, external ways to manage the environment.

        Arguments:
          package_names: The names of the packages to install.
          update: If True, update the packages if they are already installed

        Returns:
            'None' if run in jupyter, 'True' if any packages were installed, 'False' otherwise.
        """
        if isinstance(package_names, str):
            package_names = [package_names]

        env_reg = EnvironmentRegistry.instance()
        python_env: PythonRuntimeEnvironment = env_reg.environments[  # type: ignore
            "python"
        ]  # type: ignore

        if not package_names:
            package_names = OFFICIAL_KIARA_PLUGINS  # type: ignore

        if not update:
            plugin_packages: List[str] = []
            pkgs = [p.name.replace("_", "-") for p in python_env.packages]
            for package_name in package_names:
                if package_name.startswith("git:"):
                    package_name = package_name.replace("git:", "")
                    git = True
                else:
                    git = False
                package_name = package_name.replace("_", "-")
                if not package_name.startswith("kiara-plugin."):
                    package_name = f"kiara-plugin.{package_name}"

                if git or package_name.replace("_", "-") not in pkgs:
                    if git:
                        package_name = package_name.replace("-", "_")
                        plugin_packages.append(
                            f"git+https://x:x@github.com/DHARPA-project/{package_name}@develop"
                        )
                    else:
                        plugin_packages.append(package_name)
        else:
            plugin_packages = package_names  # type: ignore

        in_jupyter = "google.colab" in sys.modules or "jupyter_client" in sys.modules

        if not plugin_packages:
            if in_jupyter:
                return None
            else:
                # nothing to do
                return False

        class DummyContext(object):
            def __getattribute__(self, item):
                raise Exception(
                    "Currently installing plugins, no other operations are allowed."
                )

        current_context_name = self._current_context_alias
        for k in self._contexts.keys():
            self._contexts[k] = DummyContext()  # type: ignore
        self._current_context = DummyContext()  # type: ignore

        cmd = ["-q", "--isolated", "install"]
        if update:
            cmd.append("--upgrade")
        cmd.extend(plugin_packages)

        if in_jupyter:
            from IPython import get_ipython

            ipython = get_ipython()
            cmd_str = f"sc -l stdout = {sys.executable} -m pip {' '.join(cmd)}"
            ipython.magic(cmd_str)
            exit_code = 100
        else:
            import pip._internal.cli.main as pip

            log_message(
                "install.python_packages", packages=plugin_packages, update=update
            )
            exit_code = pip.main(cmd)

        self._contexts.clear()
        self._current_context = None
        self._current_context_alias = None

        EnvironmentRegistry._instance = None
        if current_context_name:
            self.set_active_context(context_name=current_context_name)

        if exit_code == 100:
            raise SystemExit(
                f"Please manually re-run all cells. Updated or newly installed plugin packages: {', '.join(plugin_packages)}."
            )
        elif exit_code != 0:
            raise Exception(
                f"Failed to install plugin packages: {', '.join(plugin_packages)}"
            )

        return True

    # ==================================================================================================================
    # context-management related functions
    @tag("kiara_api")
    def list_context_names(self) -> List[str]:
        """list the names of all available/registered contexts.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and
        whether we want to support single-file contexts in the future.
        """
        return list(self._kiara_config.available_context_names)

    @tag("kiara_api")
    def retrieve_context_infos(self) -> ContextInfos:
        """Retrieve information about the available/registered contexts.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and whether we want to support single-file contexts in the future.
        """
        return ContextInfos.create_context_infos(self._kiara_config.context_configs)

    @tag("kiara_api")
    def get_current_context_name(self) -> str:
        """Retrieve the name of the current context.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and whether we want to support single-file contexts in the future.
        """
        if self._current_context_alias is None:
            self.context
        return self._current_context_alias  # type: ignore

    def create_new_context(self, context_name: str, set_active: bool = True) -> None:
        """
        Create a new context.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and whether we want to support single-file contexts in the future. So if you need something like this, please let me know.

        Arguments:
            context_name: the name of the new context
            set_active: set the newly created context as the active one
        """
        if context_name in self.list_context_names():
            raise Exception(
                f"Can't create context with name '{context_name}': context already exists."
            )

        ctx = self._kiara_config.create_context(context_name, extra_pipelines=None)
        if set_active:
            self._current_context = ctx
            self._current_context_alias = context_name

        # return ctx

    @tag("kiara_api")
    def set_active_context(self, context_name: str, create: bool = False) -> None:
        """Set the currently active context for this KiarAPI instance.

        NOTE: this functionality might be changed in the future, depending on requirements and feedback and whether we want to support single-file contexts in the future.
        """

        if not context_name:
            raise Exception("No context name provided.")

        if context_name == self._current_context_alias:
            return
        if context_name not in self.list_context_names():
            if create:
                self._current_context = self._kiara_config.create_context(
                    context=context_name, extra_pipelines=None
                )
                self._current_context_alias = context_name
                return
            else:
                raise Exception(f"No context with name '{context_name}' available.")

        self._current_context = self._kiara_config.create_context(
            context=context_name, extra_pipelines=None
        )
        self._current_context_alias = context_name

    # ==================================================================================================================
    # methods for data_types

    @tag("kiara_api")
    def list_data_type_names(self, include_profiles: bool = False) -> List[str]:
        """Get a list of all registered data types.

        Arguments:
            include_profiles: if True, also include the names of all registered data type profiles
        """

        return self.context.type_registry.get_data_type_names(
            include_profiles=include_profiles
        )

    def is_internal_data_type(self, data_type_name: str) -> bool:
        """Checks if the data type is prepdominantly used internally by kiara, or whether it should be exposed to the user."""

        return self.context.type_registry.is_internal_type(
            data_type_name=data_type_name
        )

    @tag("kiara_api")
    def retrieve_data_types_info(
        self,
        filter: Union[str, Iterable[str], None] = None,
        include_data_type_profiles: bool = False,
        python_package: Union[None, str] = None,
    ) -> DataTypeClassesInfo:
        """
        Retrieve information about all data types.

        A data type is a Python class that inherits from [DataType[kiara.data_types.DataType], and it wraps a specific
        Python class that holds the actual data and provides metadata and convenience methods for managing the data internally. Data types are not directly used by users, but they are exposed in the input/output schemas of moudles and other data-related features.

        Arguments:
            filter: an optional string or (list of strings) the returned datatype ids have to match (all filters in the case of a list)
            include_data_type_profiles: if True, also include the names of all registered data type profiles
            python_package: if provided, only return data types that are defined in the given python package

        Returns:
            an object containing all information about all data types
        """

        kiara = self.context

        if python_package:
            data_type_info = kiara.type_registry.get_context_metadata(
                only_for_package=python_package
            )

            if filter:
                title = f"Filtered data types in package '{python_package}'"

                if isinstance(filter, str):
                    filter = [filter]

                filtered_types: Dict[str, DataTypeClassInfo] = {}

                for dt in data_type_info.item_infos.keys():
                    match = True

                    for f in filter:
                        if f.lower() not in dt.lower():
                            match = False
                            break
                    if match:
                        filtered_types[dt] = data_type_info.item_infos[dt]

                data_types_info = DataTypeClassesInfo(
                    group_title=title, item_infos=filtered_types
                )
                # data_types_info._kiara = kiara

            else:
                title = f"All data types in package '{python_package}'"
                data_types_info = data_type_info
                data_types_info.group_title = title
        else:
            if filter:
                if isinstance(filter, str):
                    filter = [filter]

                title = f"Filtered data_types: {filter}"
                data_type_names: Iterable[str] = []

                for m in kiara.type_registry.get_data_type_names(
                    include_profiles=include_data_type_profiles
                ):
                    match = True

                    for f in filter:

                        if f.lower() not in m.lower():
                            match = False
                            break

                    if match:
                        data_type_names.append(m)  # type: ignore
            else:
                title = "All data types"
                data_type_names = kiara.type_registry.get_data_type_names(
                    include_profiles=include_data_type_profiles
                )

            data_types = {
                d: kiara.type_registry.get_data_type_cls(d) for d in data_type_names
            }
            data_types_info = DataTypeClassesInfo.create_from_type_items(  # type: ignore
                kiara=kiara, group_title=title, **data_types
            )

        return data_types_info  # type: ignore

    @tag("kiara_api")
    def retrieve_data_type_info(self, data_type_name: str) -> DataTypeClassInfo:
        """
        Retrieve information about a specific data type.

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

    @tag("kiara_api")
    def list_module_type_names(self) -> List[str]:
        """Get a list of all registered module types."""
        return list(self.context.module_registry.get_module_type_names())

    @tag("kiara_api")
    def retrieve_module_types_info(
        self,
        filter: Union[None, str, Iterable[str]] = None,
        python_package: Union[str, None] = None,
    ) -> ModuleTypesInfo:
        """
        Retrieve information for all available module types (or a filtered subset thereof).

        A module type is Python class that inherits from [KiaraModule][kiara.modules.KiaraModule], and is the basic
        building block for processing pipelines. Module types are not used directly by users, Operations are. Operations
         are instantiated modules (meaning: the module & some (optional) configuration).

        Arguments:
            filter: an optional string (or list of string) the returned module names have to match (all filters in case of list)
            python_package: an optional string, if provided, only modules from the specified python package are returned

        Returns:
            a mapping object containing module names as keys, and information about the modules as values
        """

        if python_package:

            modules_type_info = self.context.module_registry.get_context_metadata(
                only_for_package=python_package
            )

            if filter:
                title = f"Filtered modules: {filter} (in package '{python_package}')"
                if isinstance(filter, str):
                    filter = [filter]

                filtered_types: Dict[str, ModuleTypeInfo] = {}

                for m in modules_type_info.item_infos.keys():
                    match = True

                    for f in filter:

                        if f.lower() not in m.lower():
                            match = False
                            break

                    if match:
                        filtered_types[m] = modules_type_info.item_infos[m]

                module_types_info = ModuleTypesInfo(
                    group_title=title, item_infos=filtered_types
                )
                module_types_info._kiara = self.context
            else:
                title = f"All modules in package '{python_package}'"
                module_types_info = modules_type_info
                module_types_info.group_title = title

        else:

            if filter:

                if isinstance(filter, str):
                    filter = [filter]
                title = f"Filtered modules: {filter}"
                module_types_names: Iterable[str] = []

                for m in self.context.module_registry.get_module_type_names():
                    match = True

                    for f in filter:

                        if f.lower() not in m.lower():
                            match = False
                            break

                    if match:
                        module_types_names.append(m)  # type: ignore
            else:
                title = "All modules"
                module_types_names = (
                    self.context.module_registry.get_module_type_names()
                )

            module_types = {
                n: self.context.module_registry.get_module_class(n)
                for n in module_types_names
            }

            module_types_info = ModuleTypesInfo.create_from_type_items(  # type: ignore
                kiara=self.context, group_title=title, **module_types
            )

        return module_types_info  # type: ignore

    @tag("kiara_api")
    def retrieve_module_type_info(self, module_type: str) -> ModuleTypeInfo:
        """
        Retrieve information about a specific module type.

        This can be used to retrieve information like module documentation and configuration options.

        Arguments:
            module_type: the registered name of the module

        Returns:
            an object containing all information about a module type
        """
        m_cls = self.context.module_registry.get_module_class(module_type)
        info = ModuleTypeInfo.create_from_type_class(kiara=self.context, type_cls=m_cls)
        return info

    def create_operation(
        self,
        module_type: str,
        module_config: Union[Mapping[str, Any], str, None] = None,
    ) -> Operation:
        """
        Create an [Operation][kiara.models.module.operation.Operation] instance for the specified module type and (optional) config.

        An operation is defined as a specific module type, and a specific configuration.

        This endpoint can be used to get information about the operation itself, it's inputs & outputs schemas, documentation etc.

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
            operation = create_operation(
                "pipeline", operation_config=module_config, kiara=self.context
            )
            return operation
        else:
            mc = Manifest(module_type=module_type, module_config=module_config)
            module_obj = self.context.module_registry.create_module(mc)

            return module_obj.operation

    @tag("kiara_api")
    def list_operation_ids(
        self,
        filter: Union[str, None, Iterable[str]] = None,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        operation_types: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
        python_packages: Union[str, None, Iterable[str]] = None,
    ) -> List[str]:
        """
        Get a list of all operation ids that match the specified filter.

        Arguments:
            filter: the (optional) filter string(s), an operation must match all of them to be included in the result
            input_types: each operation must have at least one input that matches one of the specified types
            output_types: each operation must have at least one output that matches one of the specified types
            operation_types: only include operations of the specified type(s)
            include_internal: whether to include operations that are predominantly used internally in kiara.
            python_packages: only include operations that are contained in one of the provided python packages
        """
        if not filter and include_internal and not python_packages:
            return sorted(self.context.operation_registry.operation_ids)

        else:
            return sorted(
                self.list_operations(
                    filter=filter,
                    input_types=input_types,
                    output_types=output_types,
                    operation_types=operation_types,
                    include_internal=include_internal,
                    python_packages=python_packages,
                ).keys()
            )

    @tag("kiara_api")
    def get_operation(
        self,
        operation: Union[Mapping[str, Any], str, Path],
        allow_external: Union[bool, None] = None,
    ) -> Operation:
        """
        Return the operation instance with the specified id.

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
        _module_type = None
        _module_config: Any = None

        if allow_external is None:
            allow_external = self.get_runtime_config().allow_external

        if isinstance(operation, Path):
            operation = operation.as_posix()

        if (
            isinstance(operation, Mapping)
            and "module_type" in operation.keys()
            and "module_config" in operation.keys()
            and not operation["module_config"]
        ):
            operation = operation["module_type"]

        if isinstance(operation, str):

            if operation in self.list_operation_ids(include_internal=True):
                _operation = self.context.operation_registry.get_operation(operation)
                return _operation

            if not allow_external:
                raise NoSuchExecutionTargetException(
                    selected_target=operation,
                    available_targets=self.context.operation_registry.operation_ids,
                    msg=f"Can't find operation with id '{operation}', and external operations are not allowed.",
                )

            if os.path.isfile(operation):
                try:
                    from kiara.models.module.pipeline import PipelineConfig

                    # we use the 'from_file' here, because that will resolve any relative paths in the pipeline
                    # if this doesn't work, we just assume the file is not a pipeline configuration but
                    # a manifest file with 'module_type' and optional 'module_config' keys
                    pipeline_conf = PipelineConfig.from_file(
                        path=operation, kiara=self.context
                    )
                    _module_config = pipeline_conf.model_dump()
                except Exception as e:
                    log_exception(e)
                    _module_config = get_data_from_file(operation)
            elif operation.startswith("http"):
                _module_config = get_data_from_url(operation)
            else:
                try:
                    _module_config = json.load(operation)  # type: ignore
                except Exception:
                    try:
                        _module_config = yaml.load(operation)  # type: ignore
                    except Exception:
                        raise Exception(
                            f"Can't parse configuration string: {operation}."
                        )
            if not isinstance(_module_config, Mapping):
                raise NoSuchExecutionTargetException(
                    selected_target=operation,
                    available_targets=self.context.operation_registry.operation_ids,
                    msg=f"Can't find operation or execution target for string '{operation}'.",
                )

        else:
            _module_config = dict(operation)  # type: ignore

        if "module_type" in _module_config.keys():
            _module_type = _module_config["module_type"]
            _module_config = _module_config.get("module_config", {})
        else:
            _module_type = "pipeline"

        op = self.create_operation(
            module_type=_module_type, module_config=_module_config
        )
        return op

    @tag("kiara_api")
    def list_operations(
        self,
        filter: Union[str, None, Iterable[str]] = None,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        operation_types: Union[str, Iterable[str], None] = None,
        python_packages: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
    ) -> "OperationsMap":
        """
        List all available operations, optionally filter.

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
        if operation_types:
            if isinstance(operation_types, str):
                operation_types = [operation_types]
            temp: Dict[str, Operation] = {}
            for op_type_name in operation_types:
                op_type = self.context.operation_registry.operation_types.get(
                    op_type_name, None
                )
                if op_type is None:
                    raise Exception(f"Operation type not registered: {op_type_name}")

                temp.update(op_type.operations)

            operations: Mapping[str, Operation] = temp
        else:
            operations = self.context.operation_registry.operations

        if filter:
            if isinstance(filter, str):
                filter = [filter]
            temp = {}
            for op_id, op in operations.items():
                match = True
                for f in filter:
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

        if python_packages:
            temp = {}
            if isinstance(python_packages, str):
                python_packages = [python_packages]
            for op_id, op in operations.items():
                info = OperationInfo.create_from_instance(
                    kiara=self.context, instance=op
                )
                pkg = info.context.labels.get("package", None)
                if pkg in python_packages:
                    temp[op_id] = op
            operations = temp

        from kiara.interfaces.python_api.models.doc import OperationsMap

        return OperationsMap.model_construct(root=operations)  # type: ignore

    @tag("kiara_api")
    def retrieve_operation_info(
        self, operation: str, allow_external: bool = False
    ) -> OperationInfo:
        """
        Return the full information for the specified operation id.

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

    @tag("kiara_api")
    def retrieve_operations_info(
        self,
        *filters: str,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        operation_types: Union[str, Iterable[str], None] = None,
        python_packages: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
    ) -> OperationGroupInfo:
        """
        Retrieve information about the matching operations.

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
        title = "Available operations"
        if filters:
            title = "Filtered operations"

        operations = self.list_operations(
            filters,
            input_types=input_types,
            output_types=output_types,
            include_internal=include_internal,
            operation_types=operation_types,
            python_packages=python_packages,
        )

        ops_info = OperationGroupInfo.create_from_operations(
            kiara=self.context, group_title=title, **operations
        )
        return ops_info

    # ==================================================================================================================
    # methods relating to pipelines

    def list_pipeline_ids(
        self,
        filter: Union[str, None, Iterable[str]] = None,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
        python_packages: Union[str, None, Iterable[str]] = None,
    ) -> List[str]:
        """
        Get a list of all pipeline (operation) ids that match the specified filter.

        Arguments:
            filter: an optional single or list of filters (all filters must match the operation id for the operation to be included)
            include_internal: also return internal pipelines
        """

        result: List[str] = self.list_operation_ids(
            filter=filter,
            input_types=input_types,
            output_types=output_types,
            operation_types=["pipeline"],
            include_internal=include_internal,
            python_packages=python_packages,
        )
        return result

    def list_pipelines(
        self,
        filter: Union[str, None, Iterable[str]] = None,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        python_packages: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
    ) -> "PipelinesMap":
        """List all available pipelines, optionally filter.

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
        from kiara.interfaces.python_api.models.doc import PipelinesMap

        ops = self.list_operations(
            filter=filter,
            input_types=input_types,
            output_types=output_types,
            operation_types=["pipeline"],
            python_packages=python_packages,
            include_internal=include_internal,
        )

        result: Dict[str, PipelineStructure] = {}
        for op in ops.values():
            details: PipelineOperationDetails = op.operation_details
            config: "PipelineConfig" = details.pipeline_config
            structure = config.structure
            result[op.operation_id] = structure

        return PipelinesMap.model_construct(root=result)

    def get_pipeline_structure(
        self,
        pipeline: Union[Mapping[str, Any], str, Path],
        allow_external: Union[bool, None] = None,
    ) -> "PipelineStructure":
        """
        Return the pipeline (Structure) instance with the specified id.

        This can be used to get information about a pipeline, like inputs/outputs scheman, documentation, included steps, stages, etc.

        The order in which the operation argument is resolved:
        - if it's a string, and an existing, registered operation_id, the associated operation is returned
        - if it's a path to an existing file, the content of the file is loaded into a dict and a pipeline operation will be created

        Arguments:
            pipeline: the pipeline id, module_type_name, path to a file, or url
            allow_external: if True, allow loading operations from external sources (e.g. a URL), if 'None' is provided, the configured value in the runtime configuration is used.

        Returns:
            pipeline structure data
        """

        op = self.get_operation(operation=pipeline, allow_external=allow_external)
        if op.module_type != "pipeline":
            raise KiaraException(
                f"Operation '{op.operation_id}' is not a pipeline, but a '{op.module_type}'"
            )
        details: PipelineOperationDetails = op.operation_details  # type: ignore
        config: "PipelineConfig" = details.pipeline_config

        return config.structure

    def retrieve_pipeline_info(
        self, pipeline: str, allow_external: bool = False
    ) -> "PipelineInfo":
        """
        Return the full information for the specified pipeline id.

        This is similar to the 'get_pipeline' method, but returns additional information. Only use this instead of
        'get_pipeline' if you need the additional info, as it's more expensive to get.

        Arguments:
            pipeline: the pipeline (operation) id

        Returns:
            augmented pipeline instance data
        """
        if not allow_external:
            op = self.context.operation_registry.get_operation(operation_id=pipeline)
        else:
            op = create_operation(module_or_operation=pipeline)

        if op.module_type != "pipeline":
            raise KiaraException(
                f"Operation '{op.operation_id}' is not a pipeline, but a '{op.module_type}'"
            )

        from kiara.models.module.pipeline.pipeline import Pipeline, PipelineInfo

        details: PipelineOperationDetails = op.operation_details  # type: ignore
        config: "PipelineConfig" = details.pipeline_config
        pipeline_instance = Pipeline(structure=config.structure, kiara=self.context)

        p_info: PipelineInfo = PipelineInfo.create_from_instance(
            kiara=self.context, instance=pipeline_instance
        )
        return p_info

    def retrieve_pipelines_info(
        self,
        *filters,
        input_types: Union[str, Iterable[str], None] = None,
        output_types: Union[str, Iterable[str], None] = None,
        python_packages: Union[str, Iterable[str], None] = None,
        include_internal: bool = False,
    ) -> "PipelineGroupInfo":
        """
        Retrieve information about the matching pipelines.

        This retrieves the same list of pipelines as [list_pipelines][kiara.interfaces.python_api.KiaraAPI.list_pipelines],
        but augments each result instance with additional information that might be useful in frontends.

        'PipelineInfo' objects contains augmented information on top of what 'normal' [PipelineStructure][kiara.models.module.pipeline.PipelineStructure] objects
        hold, but they can take longer to create/resolve. If you don't need any
        of the augmented information, just use the [list_pipelines][kiara.interfaces.python_api.KiaraAPI.list_pipelines] method
        instead.

        Arguments:
            filters: the (optional) filter strings, an operation must match all of them to be included in the result
            include_internal: whether to include operations that are predominantly used internally in kiara.
            input_types: each operation must have at least one input that matches one of the specified types
            output_types: each operation must have at least one output that matches one of the specified types
            include_internal: whether to include operations that are predominantly used internally in kiara.
            python_packages: only include operations that are contained in one of the provided python packages
        Returns:
            a wrapper object containing a dictionary of items with value_id as key, and [kiara.interfaces.python_api.models.info.OperationInfo] as value
        """

        title = "Available pipelines"
        if filters:
            title = "Filtered pipelines"

        operations = self.list_operations(
            filters,
            input_types=input_types,
            output_types=output_types,
            include_internal=include_internal,
            operation_types=["pipeline"],
            python_packages=python_packages,
        )

        from kiara.models.module.pipeline.pipeline import Pipeline, PipelineGroupInfo

        pipelines = {}
        for op_id, op in operations.items():
            details: PipelineOperationDetails = op.operation_details  # type: ignore
            config: "PipelineConfig" = details.pipeline_config
            pipeline = Pipeline(structure=config.structure, kiara=self.context)
            pipelines[op_id] = pipeline

        ps_info = PipelineGroupInfo.create_from_pipelines(
            kiara=self.context, group_title=title, **pipelines
        )
        return ps_info

    def register_pipeline(
        self,
        data: Union[Path, str, Mapping[str, Any]],
        operation_id: Union[str, None] = None,
    ) -> Operation:
        """
        Register a pipelne as new operation into this context.

        If 'operation_id' is not provided, the id will be auto-determined (in most cases using the pipeline name).

        Arguments:
            data: a dict or a path to a json/yaml file containing the definition
            operation_id: the id to use for the operation (if not specified, the id will be auto-determined)

        Returns:
            the assembled operation
        """
        return self.context.operation_registry.register_pipeline(
            data=data, operation_id=operation_id
        )

    def register_pipelines(
        self, *pipeline_paths: Union[str, Path]
    ) -> Dict[str, Operation]:
        """Register all pipelines found in the specified paths."""
        return self.context.operation_registry.register_pipelines(*pipeline_paths)

    # ==================================================================================================================
    # methods relating to values and data

    def register_data(
        self,
        data: Any,
        data_type: Union[None, str, ValueSchema, Mapping[str, Any]] = None,
        reuse_existing: bool = False,
    ) -> Value:
        """
        Register data with kiara.

        This will create a new value instance from the data and return it. The data/value itself won't be stored
        in a store, you have to use the 'store_value' function for that.

        Arguments:
            data: the data to register
            data_type: (optional) the data type of the data. If not provided, kiara will try to infer the data type.
            reuse_existing: whether to re-use an existing value that is already registered and has the same hash.

        Returns:
            a [kiara.models.values.value.Value] instance
        """
        if data_type is None:
            raise NotImplementedError(
                "Infering data types not implemented yet. Please provide one manually."
            )

        value = self.context.data_registry.register_data(
            data=data, schema=data_type, reuse_existing=reuse_existing
        )
        return value

    @tag("kiara_api")
    def list_all_value_ids(self) -> List[uuid.UUID]:
        """List all value ids in the current context.

        This returns everything, even internal values. It should be faster than using
        `list_value_ids` with equivalent parameters, because no filtering has to happen.

        Returns:
            all value_ids in the current context, using every registered store
        """

        _values = self.context.data_registry.retrieve_all_available_value_ids()
        return sorted(_values)

    @tag("kiara_api")
    def list_value_ids(self, **matcher_params: Any) -> List[uuid.UUID]:
        """
        List all available value ids for this kiara context.

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

        values = self.list_values(**matcher_params)
        return sorted((v.value_id for v in values.values()))

    @tag("kiara_api")
    def list_all_values(self) -> ValueMapReadOnly:
        """List all values in the current context, incl. internal ones.

        This should be faster than `list_values` with equivalent matcher params, because no
        filtering has to happen.
        """

        # TODO: make that parallel?
        values = {
            k: self.context.data_registry.get_value(k)
            for k in self.context.data_registry.retrieve_all_available_value_ids()
        }
        result = ValueMapReadOnly.create_from_values(
            **{str(k): v for k, v in values.items()}
        )
        return result

    @tag("kiara_api")
    def list_values(self, **matcher_params: Any) -> ValueMapReadOnly:
        """
        List all available (relevant) values, optionally filter.

        Retrieve information about all values that are available in the current kiara context session (both stored and non-stored).

        Check the `ValueMatcher` class for available parameters and defaults, for example this excludes
        internal values by default.

        Arguments:
            matcher_params: the (optional) filter parameters, check the [ValueMatcher][kiara.models.values.matchers.ValueMatcher] class for available parameters

        Returns:
            a dictionary with value_id as key, and [kiara.models.values.value.Value] as value
        """

        matcher = ValueMatcher.create_matcher(**matcher_params)
        values = self.context.data_registry.find_values(matcher=matcher)

        result = ValueMapReadOnly.create_from_values(
            **{str(k): v for k, v in values.items()}
        )
        return result

    @tag("kiara_api")
    def get_value(self, value: Union[str, Value, uuid.UUID, Path]) -> Value:
        """
        Retrieve a value instance with the specified id or alias.

        Basically a convenience method to convert any possible Python type into
        a 'Value' instance. Raises an exception if no value could be found.

        Arguments:
            value: a value id, alias or object that has a 'value_id' attribute.

        Returns:
            the Value instance
        """

        return self.context.data_registry.get_value(value=value)

    @tag("kiara_api")
    def get_values(self, **values: Union[str, Value, uuid.UUID]) -> ValueMapReadOnly:
        """Retrieve Value instances for the specified value ids or aliases.

        This is a convenience method to get fully 'hydrated' `Value` objects from references to them.

        Arguments:
            values: a dictionary with value ids or aliases as keys, and value instances as values

        Returns:
            a mapping with value_id as key, and [kiara.models.values.value.Value] as value
        """

        return self.context.data_registry.load_values(values=values)

    def query_value(
        self,
        value_or_path: Union[str, Value, uuid.UUID],
        query_path: Union[str, None] = None,
    ) -> Any:
        """
        Retrieve a value attribute with the specified id or alias.

        NOTE: This is a provisional endpoint, don't use for now, if you have a requirement that would
        be covered by this, please let me know.

        A query path is delimited by "::", and has the following format:

        ```
        <value_id_or_alias>::[<category_name>]::[<attribute_name>]::[...]
        ```

        Currently supported categories:
        - "data": the data of the value
        - "properties: the properties of the value

        If no category is specified, the value instance itself is returned.

        Raises an exception if no value could be found.

        Arguments:
            value_or_path: a value or value reference, or a query path containing the value id or alias as first token
            query_path: a query path which will be appended a potential query path computed from the first argument

        Returns:
            the attribute value
        """

        if isinstance(value_or_path, str):
            tokens = value_or_path.split(VALUE_ATTR_DELIMITER)
            value_id = tokens.pop(0)
            _value = self.get_value(value=value_id)
        else:
            tokens = []
            _value = self.get_value(value=value_or_path)

        if query_path:
            tokens.extend(query_path.split(VALUE_ATTR_DELIMITER))

        if not tokens:
            return _value

        current_result: Any = _value
        category = tokens.pop(0)
        if category == "properties":
            current_result = current_result.get_all_property_data(flatten_models=True)
        elif category == "data":
            current_result = current_result.data
        else:
            raise KiaraException(
                f"Invalid query path category: {category}. Valid categories are: {', '.join(VALID_VALUE_QUERY_CATEGORIES)}"
            )

        if tokens:
            try:
                path = VALUE_ATTR_DELIMITER.join(tokens)
                current_result = dpath.get(
                    current_result, path, separator=VALUE_ATTR_DELIMITER
                )

            except Exception:

                def dict_path(path, my_dict, all_paths):
                    for k, v in my_dict.items():
                        if isinstance(v, dict):
                            dict_path(path + "::" + k, v, all_paths)
                        else:
                            all_paths.append(path[2:] + "::" + k)

                valid_base_keys = list(current_result.keys())
                details = "Valid (base) sub-keys are:\n\n"
                for k in valid_base_keys:
                    details += f"  - {k}\n"

                all_paths: List[str] = []
                dict_path("", current_result, all_paths)

                details += "\nValid (full) sub-paths are:\n\n"
                for k in all_paths:
                    details += f"  - {k}\n"

                raise KiaraException(
                    msg=f"Failed to retrieve value attribute using query sub-path: {path}",
                    details=details,
                )

        return current_result

    @tag("kiara_api")
    def retrieve_value_info(
        self, value: Union[str, uuid.UUID, Value, Path]
    ) -> ValueInfo:
        """
        Retrieve an info object for a value.

        Companion method to 'get_value', 'ValueInfo' objects contains augmented information on top of what 'normal' [Value][kiara.models.values.value.Value] objects
        hold (like resolved properties for example), but they can take longer to create/resolve. If you don't need any
        of the augmented information, just use the [get_value][kiara.interfaces.python_api.KiaraAPI.get_value] method
        instead.

        Arguments:
            value: a value id, alias or object that has a 'value_id' attribute.

        Returns:
            the ValueInfo instance

        """
        _value = self.get_value(value=value)
        return ValueInfo.create_from_instance(kiara=self.context, instance=_value)

    @tag("kiara_api")
    def retrieve_values_info(self, **matcher_params: Any) -> ValuesInfo:
        """
        Retrieve information about the matching values.

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
        values: MutableMapping[str, Value] = self.list_values(**matcher_params)

        infos = ValuesInfo.create_from_instances(
            kiara=self.context, instances={str(k): v for k, v in values.items()}
        )
        return infos  # type: ignore

    @tag("kiara_api")
    def list_alias_names(self, **matcher_params: Any) -> List[str]:
        """
        List all available alias keys.

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
            _values = self.context.alias_registry.all_aliases
            return list(_values)

    @tag("kiara_api")
    def list_aliases(self, **matcher_params: Any) -> ValueMapReadOnly:
        """
        List all available values that have an alias assigned, optionally filter.

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
                aliases = self.context.alias_registry.find_aliases_for_value_id(
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
            all_aliases = self.context.alias_registry.all_aliases
            result = {
                k: self.context.data_registry.get_value(f"alias:{k}")
                for k in all_aliases
            }

        return ValueMapReadOnly.create_from_values(**result)

    @tag("kiara_api")
    def retrieve_aliases_info(self, **matcher_params: Any) -> ValuesInfo:
        """
        Retrieve information about the matching values.

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
            kiara=self.context, instances={str(k): v for k, v in values.items()}
        )
        return infos  # type: ignore

    def register_value_alias(
        self,
        value: Union[str, Value, uuid.UUID],
        alias: Union[str, Iterable[str]],
        allow_overwrite: bool = False,
        alias_store: Union[str, None] = None,
    ) -> None:

        self.context.alias_registry.register_aliases(
            value_id=value,
            aliases=alias,
            allow_overwrite=allow_overwrite,
            alias_store=alias_store,
        )

    def assemble_value_map(
        self,
        values: Mapping[str, Union[uuid.UUID, None, str, Value, Any]],
        values_schema: Union[None, Mapping[str, ValueSchema]] = None,
        register_data: bool = False,
        reuse_existing_data: bool = False,
    ) -> ValueMapReadOnly:
        """
        Retrive a [ValueMap][kiara.models.values.value.ValueMap] object from the provided value ids or value links.

        In most cases, this endpoint won't be used by front-ends, it's a fairly low-level method that is
        mainly used for internal purposes. If you have a use-case, let me know and I'll improve the docs
        if insufficient.

        By default, this method can only use values/datasets that are already registered in *kiara*. If you want to
        auto-register 'raw' data, you need to set the 'register_data' flag to 'True', and provide a schema for each of the fields that are not yet registered.

        Arguments:
            values: a dictionary with the values in question
            values_schema: an optional dictionary with the schema for each of the values that are not yet registered
            register_data: whether to allow auto-registration of 'raw' data
            reuse_existing_data: whether to reuse existing data with the same hash as the 'raw' data that is being registered

        Returns:
            a value map instance
        """

        if register_data:
            temp: Dict[str, Union[str, Value, uuid.UUID, None]] = {}
            for k, v in values.items():

                if isinstance(v, (Value, uuid.UUID)):
                    temp[k] = v
                    continue

                if not values_schema:
                    details = "No schema provided."
                    raise KiaraException(
                        f"Invalid field name: '{k}' (value: {v}).", details=details
                    )

                if k not in values_schema.keys():
                    details = "Valid field names: " + ", ".join(values_schema.keys())
                    raise KiaraException(
                        f"Invalid field name: '{k}' (value: {v}).", details=details
                    )

                if isinstance(v, str):

                    if v.startswith("alias:"):
                        temp[k] = v
                        continue
                    elif v.startswith("archive:"):
                        temp[k] = v
                        continue

                    try:
                        v = uuid.UUID(v)
                        temp[k] = v
                        continue
                    except Exception:
                        if v.startswith("alias:"):  # type: ignore
                            _v = v.replace("alias:", "")  # type: ignore
                        else:
                            _v = v

                        data_type = values_schema[k].type
                        if data_type != "string" and _v in self.list_aliases():
                            temp[k] = f"alias:{_v}"
                            continue

                if v is None:
                    temp[k] = None
                else:
                    _v = self.register_data(
                        data=v,
                        # data_type=values_schema[k].type,
                        data_type=values_schema[k],
                        reuse_existing=reuse_existing_data,
                    )
                    temp[k] = _v
            values = temp
        return self.context.data_registry.load_values(
            values=values, values_schema=values_schema
        )

    @tag("kiara_api")
    def store_value(
        self,
        value: Union[str, uuid.UUID, Value],
        alias: Union[str, Iterable[str], None],
        allow_overwrite: bool = True,
        store: Union[str, None] = None,
        store_related_metadata: bool = True,
        set_as_store_default: bool = False,
    ) -> StoreValueResult:
        """
        Store the specified value in a value store.

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
        # if isinstance(alias, str):
        #     alias = [alias]

        value_obj = self.get_value(value)
        persisted_data: Union[None, PersistedData] = None

        try:
            persisted_data = self.context.data_registry.store_value(
                value=value_obj, data_store=store
            )
            if alias:
                self.context.alias_registry.register_aliases(
                    value_obj,
                    alias,
                    allow_overwrite=allow_overwrite,
                    alias_store=store,
                )

            if store_related_metadata:

                from kiara.registries.metadata import MetadataMatcher

                matcher = MetadataMatcher.create_matcher(
                    reference_item_ids=[value_obj.job_id, value_obj.value_id]
                )

                target_store: MetadataStore = self.context.metadata_registry.get_archive(store)  # type: ignore
                matching_metadata = self.context.metadata_registry.find_metadata_items(
                    matcher=matcher
                )
                target_store.store_metadata_and_ref_items(matching_metadata)

            if set_as_store_default:
                store_instance = self.context.data_registry.get_archive(store)
                store_instance.set_archive_metadata_value(
                    DATA_ARCHIVE_DEFAULT_VALUE_MARKER, str(value_obj.value_id)
                )
            if isinstance(alias, str):
                alias = [alias]
            result = StoreValueResult(
                value=value_obj,
                aliases=sorted(alias) if alias else [],
                error=None,
                persisted_data=persisted_data,
            )
        except Exception as e:
            log_exception(e)
            result = StoreValueResult(
                value=value_obj,
                aliases=sorted(alias) if alias else [],
                error=(
                    str(e) if str(e) else f"Unknown error (type '{type(e).__name__}')."
                ),
                persisted_data=persisted_data,
            )

        return result

    @tag("kiara_api")
    def store_values(
        self,
        values: Union[
            str,
            Value,
            uuid.UUID,
            Mapping[str, Union[str, uuid.UUID, Value]],
            Iterable[Union[str, uuid.UUID, Value]],
        ],
        alias_map: Union[Mapping[str, Iterable[str]], bool, str] = False,
        allow_alias_overwrite: bool = True,
        store: Union[str, None] = None,
        store_related_metadata: bool = True,
    ) -> StoreValuesResult:
        """
        Store multiple values into the (default) kiara value store.

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

        if isinstance(values, (str, uuid.UUID, Value)):
            values = [values]

        result = {}
        if not isinstance(values, Mapping):
            if not alias_map:
                use_aliases = False
            elif alias_map and (alias_map is True or isinstance(alias_map, str)):

                invalid: List[Union[str, uuid.UUID, Value]] = []
                valid: Dict[str, List[str]] = {}
                for value in values:
                    if not isinstance(value, str):
                        invalid.append(value)
                        continue
                    value_id = self.context.alias_registry.find_value_id_for_alias(
                        alias=value
                    )
                    if value_id is None:
                        invalid.append(value)
                    else:
                        if alias_map is True:
                            if "#" in value:
                                new_alias = value.split("#")[1]
                            else:
                                new_alias = value
                            valid.setdefault(str(value_id), []).append(new_alias)
                        else:
                            if "#" in value:
                                new_alias = value.split("#")[1]
                            else:
                                new_alias = value
                            new_alias = f"{alias_map}{new_alias}"
                            valid.setdefault(str(value_id), []).append(new_alias)
                if invalid:
                    invalid_str = ", ".join((str(x) for x in invalid))
                    raise KiaraException(
                        msg=f"Cannot use auto-aliases with non-mapping iterable, some items are not valid aliases: {invalid_str}"
                    )
                else:
                    alias_map = valid
                    use_aliases = True
            else:
                use_aliases = True

            for value in values:

                aliases: Set[str] = set()

                value_obj = self.get_value(value)
                if use_aliases:
                    alias_key = str(value_obj.value_id)
                    alias: Union[str, None] = alias_map.get(alias_key, None)  # type: ignore
                    if alias:
                        aliases.update(alias)

                store_result = self.store_value(
                    value=value_obj,
                    alias=aliases,
                    allow_overwrite=allow_alias_overwrite,
                    store=store,
                    store_related_metadata=store_related_metadata,
                )
                result[str(value_obj.value_id)] = store_result
        else:

            for field_name, value in values.items():
                if alias_map is False:
                    aliases_map: Union[None, Iterable[str]] = None
                elif alias_map is True:
                    aliases_map = [field_name]
                elif isinstance(alias_map, str):
                    aliases_map = [f"{alias_map}.{field_name}"]
                else:
                    # means it's a mapping
                    _aliases = alias_map.get(field_name)
                    if _aliases:
                        aliases_map = list(_aliases)
                    else:
                        aliases_map = None

                value_obj = self.get_value(value)
                store_result = self.store_value(
                    value=value_obj,
                    alias=aliases_map,
                    allow_overwrite=allow_alias_overwrite,
                    store=store,
                    store_related_metadata=store_related_metadata,
                )
                result[field_name] = store_result

        return StoreValuesResult(root=result)

    # ------------------------------------------------------------------------------------------------------------------
    # archive-related methods
    @tag("kiara_api")
    def import_values(
        self,
        source_archive: Union[str, Path],
        values: Union[
            str,
            Mapping[str, Union[str, uuid.UUID, Value]],
            Iterable[Union[str, uuid.UUID, Value]],
        ],
        alias_map: Union[Mapping[str, Iterable[str]], bool, str] = False,
        allow_alias_overwrite: bool = True,
        source_registered_name: Union[str, None] = None,
    ) -> StoreValuesResult:
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

        if source_archive in [None, DEFAULT_STORE_MARKER]:
            raise KiaraException(
                "You cannot use the default store as source for this operation."
            )

        if alias_map is True:
            pass
        elif alias_map is False:
            pass
        elif isinstance(alias_map, str):
            pass
        elif isinstance(alias_map, Mapping):
            pass
        else:
            raise KiaraException(
                f"Invalid type for 'alias_map' argument: {type(alias_map)}."
            )

        source_archive_ref = self.register_archive(
            archive=source_archive,  # type: ignore
            registered_name=source_registered_name,
            create_if_not_exists=False,
            allow_write_access=False,
            existing_ok=True,
        )

        value_ids: Set[uuid.UUID] = set()
        aliases: Set[str] = set()

        if isinstance(values, str):
            values = [values]

        if not isinstance(values, Mapping):
            # means we have a list of value ids/aliases
            for value in values:
                if isinstance(value, uuid.UUID):
                    value_ids.add(value)
                elif isinstance(value, str):
                    try:
                        _value = uuid.UUID(value)
                        value_ids.add(_value)
                    except Exception:
                        aliases.add(value)
        else:
            raise NotImplementedError("Not implemented yet.")

        new_values: Dict[str, Union[uuid.UUID, str]] = {}
        idx = 0
        for value_id in value_ids:
            field = f"field_{idx}"
            idx += 1
            new_values[field] = value_id

        new_alias_map = {}
        for alias in aliases:
            field = f"field_{idx}"
            idx += 1
            new_values[field] = f"{source_archive_ref}#{alias}"
            if alias_map is False:
                pass
            elif alias_map is True:
                new_alias_map[field] = [f"{alias}"]
            elif isinstance(alias_map, str):
                new_alias_map[field] = [f"{alias_map}{alias}"]
            else:
                # means its a dict
                if alias in alias_map.keys():
                    for a in alias_map[alias]:
                        new_alias_map.setdefault(field, []).append(a)

        result: StoreValuesResult = self.store_values(
            values=new_values,
            alias_map=new_alias_map,
            allow_alias_overwrite=allow_alias_overwrite,
        )
        return result

    @tag("kiara_api")
    def export_values(
        self,
        target_archive: Union[str, Path],
        values: Union[
            str,
            Value,
            uuid.UUID,
            Mapping[str, Union[str, uuid.UUID, Value]],
            Iterable[Union[str, uuid.UUID, Value]],
        ],
        alias_map: Union[Mapping[str, Iterable[str]], bool, str] = False,
        allow_alias_overwrite: bool = True,
        target_registered_name: Union[str, None] = None,
        append: bool = False,
        target_store_params: Union[None, Mapping[str, Any]] = None,
        export_related_metadata: bool = True,
        additional_archive_metadata: Union[None, Mapping[str, Any]] = None,
    ) -> StoreValuesResult:
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

        if target_archive in [None, DEFAULT_STORE_MARKER]:
            raise KiaraException(
                "You cannot use the default store as target for this operation."
            )

        if target_store_params is None:
            target_store_params = {}

        target_archive_ref = self.register_archive(
            archive=target_archive,  # type: ignore
            registered_name=target_registered_name,
            create_if_not_exists=True,
            allow_write_access=True,
            existing_ok=True if append else False,
            **target_store_params,
        )

        result: StoreValuesResult = self.store_values(
            values=values,
            alias_map=alias_map,
            allow_alias_overwrite=allow_alias_overwrite,
            store=target_archive_ref,
            store_related_metadata=export_related_metadata,
        )

        if additional_archive_metadata:
            for k, v in additional_archive_metadata.items():
                self.set_archive_metadata_value(target_archive_ref, k, v)

        return result

    def register_archive(
        self,
        archive: Union[str, Path, "KiArchive"],
        allow_write_access: bool = False,
        registered_name: Union[str, None] = None,
        create_if_not_exists: bool = True,
        existing_ok: bool = True,
        **create_params: Any,
    ) -> str:
        """Register a kiarchive with the current context.

        In most cases, this will be used to 'load' an existing kiarchive file and attach it to the current context.
        If the file does not exist, one will be created, with the filename (without '.kiarchive' suffix) as the archive name if not specified.

        In the future this might also take a URL, but for now only local files are supported.

        # NOTE: this is a preliminary endpoint, and might be changed in the future. If you have a use-case for this, please let me know.

        Arguments:
            archive: the uri of the archive (file path), or a [Kiarchive][kiara.interfaces.python_api.models.archive.Kiarchive] instance
            allow_write_access: whether to allow write access to the archive
            registered_name: the name/alias that the archive is registered in the context, and which can be used in the 'store_value(s)' endpoint, if not provided, it will be auto-determined from the file name
            create_if_not_exists: if the file does not exist, create it. If this is 'False', an exception will be raised if the file does not exist.
            existing_ok: whether the file is allowed to exist already, if 'False', an exception will be raised if the file exists
            create_params: additional parameters to pass to the 'create_kiarchive' method if the file does not exist yet

        Returns:
            the name/alias that the archive is registered in the context, and which can be used in the 'store_value(s)' endpoint
        """
        from kiara.interfaces.python_api.models.archive import KiArchive

        if not existing_ok and not create_if_not_exists:
            raise KiaraException(
                "Both 'existing_ok' and 'create_if_not_exists' cannot be 'False' at the same time."
            )

        if isinstance(archive, str):
            archive = Path(archive)

        if isinstance(archive, Path):

            if not archive.name.endswith(".kiarchive"):
                archive = archive.parent / f"{archive.name}.kiarchive"

            if archive.exists():
                if not existing_ok:
                    raise KiaraException(
                        f"Archive file '{archive.as_posix()}' already exists."
                    )
                archive = KiArchive.load_kiarchive(
                    kiara=self.context,
                    path=archive,
                    archive_name=registered_name,
                    allow_write_access=allow_write_access,
                )
                log_message("archive.loaded", archive_name=archive.archive_name)
            else:
                if not create_if_not_exists:
                    raise KiaraException(
                        f"Archive file '{archive.as_posix()}' does not exist."
                    )
                kiarchive_alias = archive.name
                if kiarchive_alias.endswith(".kiarchive"):
                    kiarchive_alias = kiarchive_alias[:-10]

                compression: Union[None, CHUNK_COMPRESSION_TYPE, str] = None
                for k, v in create_params.items():
                    if k == "compression":
                        compression = v
                    else:
                        raise KiaraException(
                            msg=f"Invalid archive creation parameter: '{k}'."
                        )

                archive = KiArchive.create_kiarchive(
                    kiara=self.context,
                    kiarchive_uri=archive.as_posix(),
                    allow_existing=False,
                    archive_name=kiarchive_alias,
                    allow_write_access=allow_write_access,
                    compression=compression,
                )
                log_message("archive.created", archive_name=archive.archive_name)

        else:
            raise NotImplementedError("Only local files are supported for now.")

        data_archive = archive.data_archive
        assert data_archive is not None
        data_alias = self.context.register_external_archive(
            data_archive,
            allow_write_access=allow_write_access,
        )

        alias_archive = archive.alias_archive
        assert alias_archive is not None
        alias_alias = self.context.register_external_archive(
            alias_archive, allow_write_access=allow_write_access
        )

        job_archive = archive.job_archive
        assert job_archive is not None
        job_alias = self.context.register_external_archive(
            job_archive, allow_write_access=allow_write_access
        )

        metadata_archive = archive.metadata_archive
        assert metadata_archive is not None
        metadata_alias = self.context.register_external_archive(
            metadata_archive, allow_write_access=allow_write_access
        )
        assert data_alias["data"] == alias_alias["alias"]
        assert data_alias["data"] == job_alias["job_record"]
        assert data_alias["data"] == metadata_alias["metadata"]
        assert archive.archive_name == data_alias["data"]

        return archive.archive_name

    def set_archive_metadata_value(
        self,
        archive: Union[str, uuid.UUID],
        key: str,
        value: Any,
        archive_type: Literal["data", "alias", "job_record", "metadata"] = "data",
    ) -> None:
        """Add metadata to an archive.

        Note that this is different to adding metadata to a context, since it is attached directly
        to a special section of the archive itself.
        """

        if archive_type == "data":
            _archive: Union[None, KiaraArchive] = (
                self.context.data_registry.get_archive(archive)
            )
            if _archive is None:
                raise KiaraException(f"Archive '{archive}' does not exist.")
            _archive.set_archive_metadata_value(key, value)
        elif archive_type == "alias":
            _archive = self.context.alias_registry.get_archive(archive)
            if _archive is None:
                raise KiaraException(f"Archive '{archive}' does not exist.")
            _archive.set_archive_metadata_value(key, value)
        elif archive_type == "metadata":
            _archive = self.context.metadata_registry.get_archive(archive)
            if _archive is None:
                raise KiaraException(f"Archive '{archive}' does not exist.")
            _archive.set_archive_metadata_value(key, value)
        elif archive_type == "job_record":
            _archive = self.context.job_registry.get_archive(archive)
            if _archive is None:
                raise KiaraException(f"Archive '{archive}' does not exist.")
            _archive.set_archive_metadata_value(key, value)
        else:
            raise KiaraException(
                f"Invalid archive type: {archive_type}. Valid types are: 'data', 'alias'."
            )

    @tag("kiara_api")
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

        from kiara.interfaces.python_api.models.archive import KiArchive
        from kiara.models.archives import KiArchiveInfo

        if not isinstance(archive, KiArchive):
            archive = KiArchive.load_kiarchive(kiara=self.context, path=archive)

        kiarchive_info = KiArchiveInfo.create_from_instance(
            kiara=self.context, instance=archive
        )
        return kiarchive_info

    @tag("kiara_api")
    def export_archive(
        self,
        target_archive: Union[str, Path],
        target_registered_name: Union[str, None] = None,
        append: bool = False,
        no_aliases: bool = False,
        target_store_params: Union[None, Mapping[str, Any]] = None,
    ) -> StoreValuesResult:
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

        result = self.copy_archive(
            source_archive=DEFAULT_STORE_MARKER,
            target_archive=target_archive,
            target_registered_name=target_registered_name,
            append=append,
            target_store_params=target_store_params,
            no_aliases=no_aliases,
        )
        return result

    @tag("kiara_api")
    def import_archive(
        self,
        source_archive: Union[str, Path],
        source_registered_name: Union[str, None] = None,
        no_aliases: bool = False,
    ) -> StoreValuesResult:
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

        result = self.copy_archive(
            source_archive=source_archive,
            target_archive=DEFAULT_STORE_MARKER,
            source_registered_name=source_registered_name,
            no_aliases=no_aliases,
        )
        return result

    def copy_archive(
        self,
        source_archive: Union[None, str, Path],
        target_archive: Union[None, str, Path] = None,
        source_registered_name: Union[str, None] = None,
        target_registered_name: Union[str, None] = None,
        append: bool = False,
        no_aliases: bool = False,
        target_store_params: Union[None, Mapping[str, Any]] = None,
    ) -> StoreValuesResult:
        """Import all data from the specified archive into the current context.

        The archives will be registered into the context, either using the provided registered_name, otherwise the name
        will be auto-determined from the archive metadata.

        Currently, this only works with an external archive file, not with an archive that is registered into the context.
        This will be added later on.

        The one supported value for the 'target_store_params' argument currently is 'compression', which can be one of:

        - zstd: zstd compression (default) -- fairly fast, and good compression
        - none: no compression
        - LZMA: LZMA compression -- very slow, but very good compression
        - LZ4: LZ4 compression -- very fast, but not as good compression as zstd

        This method does not raise an error if the storing of the value fails, so you have to investigate the
        'StoreValuesResult' instance that is returned to see if the storing was successful

        Arguments:
            source_archive: the registered_name or uri of the source archive, if None, the context default data/alias store will be used
            target_archive: the registered_name or uri of the target archive, defaults to the context default data/alias store
            source_registered_name: the name/alias that the archive should be registered in the context (if necessary)
            target_registered_name: the name/alias that the archive should be registered in the context (if necessary)
            append: whether to append to an existing archive or error out if the target already exists
            no_aliases: whether to skip importing aliases
            target_store_params: additional parameters to pass to the 'create_kiarchive' method if the target file does not exist yet

        Returns:
            an object outlining which values (identified by the specified value key or an enumerated index) where stored and how

        """

        if source_archive in [None, DEFAULT_STORE_MARKER]:
            source_archive_ref = DEFAULT_STORE_MARKER
        else:
            source_archive_ref = self.register_archive(
                archive=source_archive,  # type: ignore
                registered_name=source_registered_name,
                create_if_not_exists=False,
                existing_ok=True,
            )

        if target_archive in [None, DEFAULT_STORE_MARKER]:
            target_archive_ref = DEFAULT_STORE_MARKER
        else:
            if target_store_params is None:
                target_store_params = {}
            target_archive_ref = self.register_archive(
                archive=target_archive,  # type: ignore
                registered_name=target_registered_name,
                create_if_not_exists=True,
                allow_write_access=True,
                existing_ok=True if append else False,
                **target_store_params,
            )

        if source_archive_ref == target_archive_ref:
            raise KiaraException(
                f"Source and target archive cannot be the same: {source_archive_ref} != {target_archive_ref}"
            )

        source_values = self.list_values(
            in_data_archives=[source_archive_ref], allow_internal=True, has_alias=False
        ).values()

        if not no_aliases:
            aliases = self.list_aliases(in_data_archives=[source_archive_ref])
            alias_map: Union[bool, Dict[str, List[str]]] = {}
            for alias, value in aliases.items():

                if source_archive_ref != DEFAULT_STORE_MARKER:
                    # TODO: maybe add a matcher arg to the list_aliases endpoint
                    if not alias.startswith(f"{source_archive_ref}#"):
                        continue
                    alias_map.setdefault(str(value.value_id), []).append(  # type: ignore
                        alias[len(source_archive_ref) + 1 :]
                    )
                else:
                    if "#" in alias:
                        continue
                    alias_map.setdefault(str(value.value_id), []).append(alias)  # type: ignore
        else:
            alias_map = False

        result: StoreValuesResult = self.store_values(
            source_values, alias_map=alias_map, store=target_archive_ref
        )
        return result

    # ------------------------------------------------------------------------------------------------------------------
    # operation-related methods

    def get_operation_type(
        self, op_type: Union[str, Type[OperationType]]
    ) -> OperationType:
        """Get the management object for the specified operation type."""
        return self.context.operation_registry.get_operation_type(op_type=op_type)

    def retrieve_operation_type_info(
        self, op_type: Union[str, Type[OperationType]]
    ) -> OperationTypeInfo:
        """Get an info object for the specified operation type."""
        _op_type = self.get_operation_type(op_type=op_type)
        return OperationTypeInfo.create_from_type_class(
            kiara=self.context, type_cls=_op_type.__class__
        )

    def find_operation_id(
        self, module_type: str, module_config: Union[None, Mapping[str, Any]] = None
    ) -> Union[None, str]:
        """
        Try to find the registered operation id for the specified module type and configuration.

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
    ) -> "PipelineConfig":
        """
        Assemble a (pipeline) module config to filter values of a specific data type.

        NOTE: this is a preliminary endpoint, and might go away in the future. If you have a need for this
        functionality, please let me know your requirements and we can work on fleshing this out.

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
        filter_op_type: FilterOperationType = self.context.operation_registry.get_operation_type("filter")  # type: ignore
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

    # ------------------------------------------------------------------------------------------------------------------
    # metadata-related methods

    def register_metadata_item(
        self, key: str, value: str, store: Union[str, None] = None
    ) -> uuid.UUID:
        """Register a metadata item into the specified metadata store.

        Currently, this allows you to store comments within the default kiara context. You can use any string,
        as key, for example a stringified `job_id`, or `value_id`, or any other string that makes sense in
        the context you are using this in.

        If you use the store argument, the store needs to be mounted into the current *kiara* context. For now,
        you can ignore this and not provide any value here, since this area is still in flux. If you need
        to store a metadata item into an external context, and you can't figure out how to do it,
        let me know.

        Note: this is preliminary and subject to change based on your input, so please provide your thoughts

        Arguments:
            key: the key under which to store the metadata (can be anything you can think of)
            value: the comment you want to store
            store: the store to use, by default the context default is used

        Returns:
            a globally unique identifier for the metadata item
        """

        if not value:
            raise KiaraException("Cannot store empty metadata item.")

        from kiara.models.metadata import CommentMetadata

        item = CommentMetadata(comment=value)

        return self.context.metadata_registry.register_metadata_item(
            key=key, item=item, store=store
        )

    def find_metadata_items(self, **matcher_params: Any):

        from kiara.registries.metadata import MetadataMatcher

        matcher = MetadataMatcher.create_matcher(**matcher_params)

        return self.context.metadata_registry.find_metadata_items(matcher=matcher)

    # ------------------------------------------------------------------------------------------------------------------
    # render-related methods

    def retrieve_renderer_infos(
        self, source_type: Union[str, None] = None, target_type: Union[str, None] = None
    ) -> RendererInfos:
        """Retrieve information about the available renderers.

        Note: this is preliminary and mainly used in the cli, if another use-case comes up let me know and I'll make this more generic, and an 'official' endpoint.

        Arguments:
            source_type: the type of the item to render (optional filter)
            target_type: the type/profile of the rendered result (optional filter)

        Returns:
            a wrapper object containing the items as dictionary with renderer alias as key, and [kiara.interfaces.python_api.models.info.RendererInfo] as value

        """

        if not source_type and not target_type:
            renderers = self.context.render_registry.registered_renderers
        elif source_type and not target_type:
            renderers = self.context.render_registry.retrieve_renderers_for_source_type(
                source_type=source_type
            )
        elif target_type and not source_type:
            raise KiaraException(msg="Cannot retrieve renderers for target type only.")
        else:
            renderers = self.context.render_registry.retrieve_renderers_for_source_target_combination(
                source_type=source_type, target_type=target_type  # type: ignore
            )

        group = {k.get_renderer_alias(): k for k in renderers}
        infos = RendererInfos.create_from_instances(kiara=self.context, instances=group)
        return infos  # type: ignore

    def retrieve_renderers_for(self, source_type: str) -> List[KiaraRenderer]:
        """Retrieve available renderer instances for a specific data type.

        Note: this is not preliminary, and, mainly used in the cli, if another use-case comes up let me know and I'll make this more generic, and an 'official' endpoint.
        """

        return self.context.render_registry.retrieve_renderers_for_source_type(
            source_type=source_type
        )

    def render(
        self,
        item: Any,
        source_type: str,
        target_type: str,
        render_config: Union[Mapping[str, Any], None] = None,
    ) -> Any:
        """Render an internal instance of a supported source type into one of the supported target types.

        Note: this is not preliminary, and, mainly used in the cli, if another use-case comes up let me know and I'll make this more generic, and an 'official' endpoint.

        To find out the supported source/target combinations, you can use the kiara cli:

        ```
        kiara render list-renderers
        ```
        or, for a filtered list:
        ````
        kiara render --source-type pipeline list-renderers
        ```

        What Python types are actually supported for the 'item' argument depends on the source_type of the renderer you are calling, for example if that is a pipeline, most of the ways to specify a pipeline would be supported (operation_id, pipeline file, etc.). This might need more documentation, let me know what exactly is needed in a support ticket and I'll add that information.

        Arguments:
            item: the item to render
            source_type: the type of the item to render
            target_type: the type/profile of the rendered result
            render_config: optional configuration, depends on the renderer that is called

        """

        registry = self.context.render_registry
        result = registry.render(
            item=item,
            source_type=source_type,
            target_type=target_type,
            render_config=render_config,
        )
        return result

    def assemble_render_pipeline(
        self,
        data_type: str,
        target_format: Union[str, Iterable[str]] = "string",
        filters: Union[None, str, Iterable[str], Mapping[str, str]] = None,
        use_pretty_print: bool = False,
    ) -> Operation:
        """
        Create a manifest describing a transformation that renders a value of the specified data type in the target format.

        NOTE: this is a preliminary endpoint, don't use in anger yet.

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
                self.context.operation_registry.get_operation_type("pretty_print")
            )  # type: ignore
            ops = pretty_print_op_type.get_target_types_for(data_type)
        else:
            render_op_type: (
                RenderValueOperationType
            ) = self.context.operation_registry.get_operation_type(
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
                module_type="pipeline", module_config=pipeline_config.model_dump()
            )
            module = self.context.module_registry.create_module(manifest=manifest)
            operation = Operation.create_from_module(module, doc=pipeline_config.doc)
        else:
            operation = match

        return operation

    # ------------------------------------------------------------------------------------------------------------------
    # job-related methods
    def queue_manifest(
        self,
        manifest: Manifest,
        inputs: Union[None, Mapping[str, Any]] = None,
        **job_metadata: Any,
    ) -> uuid.UUID:
        """
        Queue a job using the provided manifest to describe the module and config that should be executed.

        You probably want to use 'queue_job' instead.

        Arguments:
            manifest: the manifest
            inputs: the job inputs (can be either references to values, or raw inputs

        Returns:
            a result value map instance
        """

        if self.context.runtime_config.runtime_profile == "dharpa":
            if not job_metadata:
                raise Exception(
                    "No job metadata provided. You need to provide a 'comment' argument when running your job."
                )

            if "comment" not in job_metadata.keys():
                raise KiaraException(msg="You need to provide a 'comment' for the job.")

            save_values = True
        else:
            save_values = False

        if inputs is None:
            inputs = {}

        job_config = self.context.job_registry.prepare_job_config(
            manifest=manifest, inputs=inputs
        )

        job_id = self.context.job_registry.execute_job(
            job_config=job_config, wait=False, auto_save_result=save_values
        )

        if job_metadata:
            self.context.metadata_registry.register_job_metadata_items(
                job_id=job_id, items=job_metadata
            )

        return job_id

    def run_manifest(
        self,
        manifest: Manifest,
        inputs: Union[None, Mapping[str, Any]] = None,
        **job_metadata: Any,
    ) -> ValueMapReadOnly:
        """
        Run a job using the provided manifest to describe the module and config that should be executed.

        You probably want to use 'run_job' instead.

        Arguments:
            manifest: the manifest
            inputs: the job inputs (can be either references to values, or raw inputs
            job_metadata: additional metadata to store with the job

        Returns:
            a result value map instance
        """
        job_id = self.queue_manifest(manifest=manifest, inputs=inputs, **job_metadata)
        return self.context.job_registry.retrieve_result(job_id=job_id)

    def queue_job(
        self,
        operation: Union[str, Path, Manifest, OperationInfo, JobDesc],
        inputs: Union[Mapping[str, Any], None],
        operation_config: Union[None, Mapping[str, Any]] = None,
        **job_metadata: Any,
    ) -> uuid.UUID:
        """
        Queue a job from a operation id, module_name (and config), or pipeline file, wait for the job to finish and retrieve the result.

        This is a convenience method that auto-detects what is meant by the 'operation' string input argument.

        If the 'operation' is a JobDesc instance, and that JobDesc instance has the 'save' attribute
        set, it will be ignored, so you'll have to store any results manually.

        Arguments:
            operation: a module name, operation id, or a path to a pipeline file (resolved in this order, until a match is found)..
            inputs: the operation inputs
            operation_config: the (optional) module config in case 'operation' is a module name
            job_metadata: additional metadata to store with the job

        Returns:
            the queued job id
        """

        if inputs is None:
            inputs = {}

        if isinstance(operation, str):
            if os.path.isfile(operation):
                job_path = Path(operation)
                if not job_path.is_file():
                    raise Exception(
                        f"Can't queue job from file '{job_path.as_posix()}': file does not exist/not a file."
                    )

                op_data = get_data_from_file(job_path)
                if isinstance(op_data, Mapping) and "operation" in op_data.keys():
                    try:
                        repl_dict: Dict[str, Any] = {
                            "this_dir": job_path.parent.as_posix()
                        }
                        job_data = replace_var_names_in_obj(
                            op_data, repl_dict=repl_dict
                        )
                        job_data["job_alias"] = job_path.stem
                        job_desc = JobDesc(**job_data)
                        _operation: Union[Manifest, str] = job_desc.get_operation(
                            kiara_api=self
                        )
                        if job_desc.inputs:
                            _inputs = dict(job_desc.inputs)
                            _inputs.update(inputs)
                            inputs = _inputs
                    except Exception as e:
                        raise KiaraException(
                            f"Failed to parse job description file: {operation}",
                            parent=e,
                        )
                else:
                    _operation = job_path.as_posix()
            else:
                _operation = operation
        elif isinstance(operation, Path):
            if not operation.is_file():
                raise Exception(
                    f"Can't queue job from file '{operation.as_posix()}': file does not exist/not a file."
                )
            _operation = operation.as_posix()
        elif isinstance(operation, OperationInfo):
            _operation = operation.operation
        elif isinstance(operation, JobDesc):
            if operation_config:
                raise KiaraException(
                    "Specifying 'operation_config' when operation is a job_desc is invalid."
                )
            _operation = operation.get_operation(kiara_api=self)
            if operation.inputs:
                _inputs = dict(operation.inputs)
                _inputs.update(inputs)
                inputs = _inputs
        else:
            _operation = operation

        if not isinstance(_operation, Manifest):
            manifest: Manifest = create_operation(
                module_or_operation=_operation,
                operation_config=operation_config,
                kiara=self.context,
            )
        else:
            manifest = _operation

        job_id = self.queue_manifest(manifest=manifest, inputs=inputs, **job_metadata)

        return job_id

    def run_job(
        self,
        operation: Union[str, Path, Manifest, OperationInfo, JobDesc],
        inputs: Union[None, Mapping[str, Any]] = None,
        operation_config: Union[None, Mapping[str, Any]] = None,
        **job_metadata: Any,
    ) -> ValueMapReadOnly:
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
            operation_config: the (optional) module config in case 'operation' is a module name
            **job_metadata: additional metadata to store with the job

        Returns:
            the job result value map

        """
        if inputs is None:
            inputs = {}

        job_id = self.queue_job(
            operation=operation,
            inputs=inputs,
            operation_config=operation_config,
            **job_metadata,
        )
        return self.context.job_registry.retrieve_result(job_id=job_id)

    @tag("kiara_api")
    def get_job(self, job_id: Union[str, uuid.UUID]) -> "ActiveJob":
        """Retrieve the status of the job with the provided id."""
        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)

        job_status = self.context.job_registry.get_job(job_id=job_id)
        return job_status

    @tag("kiara_api")
    def get_job_result(self, job_id: Union[str, uuid.UUID]) -> ValueMapReadOnly:
        """Retrieve the result(s) of the specified job."""
        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)

        result = self.context.job_registry.retrieve_result(job_id=job_id)
        return result

    @tag("kiara_api")
    def list_all_job_record_ids(self) -> List[uuid.UUID]:
        """List all available job ids in this kiara context, ordered from newest to oldest, including internal jobs.

        This should be faster than `list_job_record_ids` with equivalent parameters, because no filtering
        needs to be done.
        """

        job_ids = self.context.job_registry.retrieve_all_job_record_ids()
        return job_ids

    @tag("kiara_api")
    def list_job_record_ids(self, **matcher_params: Any) -> List[uuid.UUID]:
        """List all available job ids in this kiara context, ordered from newest to oldest.

        You can look up the supported matcher parameter arguments via the [JobMatcher][kiara.models.module.jobs.JobMatcher] class. By default, this method for example
        does not return jobs marked as 'internal'.

        Arguments:
            matcher_params: additional parameters to pass to the job matcher

        Returns:
            a list of job ids, ordered from latest to earliest
        """

        job_ids = list(self.list_job_records(**matcher_params).keys())
        return job_ids

    @tag("kiara_api")
    def list_all_job_records(self) -> Mapping[uuid.UUID, "JobRecord"]:
        """List all available job records in this kiara context, ordered from newest to oldest, including internal jobs.

        This should be faster than `list_job_records` with equivalent parameters, because no filtering
        needs to be done.
        """

        job_records = self.context.job_registry.retrieve_all_job_records()
        return job_records

    @tag("kiara_api")
    def list_job_records(
        self, **matcher_params: Any
    ) -> Mapping[uuid.UUID, "JobRecord"]:
        """List all available job ids in this kiara context, ordered from newest to oldest.

        You can look up the supported matcher parameter arguments via the [JobMatcher][kiara.models.module.jobs.JobMatcher] class. By default, this method for example
        does not return jobs marked as 'internal'.

        You can look up the supported matcher parameter arguments via the [JobMatcher][kiara.models.module.jobs.JobMatcher] class.

        Arguments:
            matcher_params: additional parameters to pass to the job matcher

        Returns:
            a list of job details, ordered from latest to earliest

        """

        from kiara.models.module.jobs import JobMatcher

        matcher = JobMatcher(**matcher_params)
        job_records = self.context.job_registry.find_job_records(matcher=matcher)

        return job_records

    @tag("kiara_api")
    def get_job_record(self, job_id: Union[str, uuid.UUID]) -> Union["JobRecord", None]:
        """Retrieve the detailed job record for the specified job id.

        If no job can be found, 'None' is returned.
        """

        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)

        job_record = self.context.job_registry.get_job_record(job_id=job_id)
        return job_record

    def render_value(
        self,
        value: Union[str, uuid.UUID, Value],
        target_format: Union[str, Iterable[str]] = "string",
        filters: Union[None, Iterable[str], Mapping[str, str]] = None,
        render_config: Union[Mapping[str, str], None] = None,
        add_root_scenes: bool = True,
        use_pretty_print: bool = False,
    ) -> RenderValueResult:
        """
        Render a value in the specified target format.

        NOTE: this is a preliminary endpoint, don't use in anger yet.

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
                use_pretty_print=use_pretty_print,
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
                f"Could not find render operation for value '{_value.value_id}', type: {_value.value_schema.type}"
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
            value_render_data: RenderValueResult = render_result.data
        else:
            render_result = result["render_value_result"]

            if render_result.data_type_name != "render_value_result":
                raise Exception(
                    f"Invalid result type for render operation: {render_result.data_type_name}"
                )

            value_render_data = render_result.data  # type: ignore

        return value_render_data

    # ------------------------------------------------------------------------------------------------------------------
    # workflow-related methods
    # all of the workflow-related methods are provisional experiments, so don't rely on them to be availale long term

    def list_workflow_ids(self) -> List[uuid.UUID]:
        """List all available workflow ids.

        NOTE: this is a provisional endpoint, don't use in anger yet
        """
        return list(self.context.workflow_registry.all_workflow_ids)

    def list_workflow_alias_names(self) -> List[str]:
        """ "List all available workflow aliases.

        NOTE: this is a provisional endpoint, don't use in anger yet
        """
        return list(self.context.workflow_registry.workflow_aliases.keys())

    def get_workflow(
        self, workflow: Union[str, uuid.UUID], create_if_necessary: bool = True
    ) -> "Workflow":
        """Retrieve the workflow instance with the specified id or alias.

        NOTE: this is a provisional endpoint, don't use in anger yet
        """
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

    def retrieve_workflow_info(
        self, workflow: Union[str, uuid.UUID, "Workflow"]
    ) -> WorkflowInfo:
        """Retrieve information about the specified workflow.

        NOTE: this is a provisional endpoint, don't use in anger yet
        """

        from kiara.interfaces.python_api.workflow import Workflow

        if isinstance(workflow, Workflow):
            _workflow: Workflow = workflow
        else:
            _workflow = self.get_workflow(workflow)

        return WorkflowInfo.create_from_workflow(workflow=_workflow)

    def list_workflows(self, **matcher_params) -> "WorkflowsMap":
        """List all available workflow sessions, indexed by their unique id."""

        from kiara.interfaces.python_api.models.doc import WorkflowsMap
        from kiara.interfaces.python_api.models.workflow import WorkflowMatcher

        workflows = {}

        matcher = WorkflowMatcher(**matcher_params)
        if matcher.has_alias:
            for (
                alias,
                workflow_id,
            ) in self.context.workflow_registry.workflow_aliases.items():

                workflow = self.get_workflow(workflow=workflow_id)
                workflows[workflow.workflow_id] = workflow
            return WorkflowsMap(root={str(k): v for k, v in workflows.items()})
        else:
            for workflow_id in self.context.workflow_registry.all_workflow_ids:
                workflow = self.get_workflow(workflow=workflow_id)
                workflows[workflow_id] = workflow
            return WorkflowsMap(root={str(k): v for k, v in workflows.items()})

    def list_workflow_aliases(self, **matcher_params) -> "WorkflowsMap":
        """List all available workflow sessions that have an alias, indexed by alias.

        NOTE: this is a provisional endpoint, don't use in anger yet
        """

        from kiara.interfaces.python_api.models.doc import WorkflowsMap
        from kiara.interfaces.python_api.workflow import Workflow

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

        return WorkflowsMap(root=result)

    def retrieve_workflows_info(self, **matcher_params: Any) -> WorkflowGroupInfo:
        """Get a map info instances for all available workflows, indexed by (stringified) workflow-id.

        NOTE: this is a provisional endpoint, don't use in anger yet
        """
        workflows = self.list_workflows(**matcher_params)

        workflow_infos = WorkflowGroupInfo.create_from_workflows(
            *workflows.values(),
            group_title=None,
            alias_map=self.context.workflow_registry.workflow_aliases,
        )
        return workflow_infos

    def retrieve_workflow_aliases_info(
        self, **matcher_params: Any
    ) -> WorkflowGroupInfo:
        """Get a map info instances for all available workflows, indexed by alias.

        NOTE: this is a provisional endpoint, don't use in anger yet
        """
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
        initial_pipeline: Union[None, Path, str, Mapping[str, Any]] = None,
        initial_inputs: Union[None, Mapping[str, Any]] = None,
        documentation: Union[Any, None] = None,
        save: bool = False,
        force_alias: bool = False,
    ) -> "Workflow":
        """Create a workflow instance.

        NOTE: this is a provisional endpoint, don't use in anger yet
        """

        from kiara.interfaces.python_api.workflow import Workflow

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
            operation = self.get_operation(operation=initial_pipeline)
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

    def _repr_html_(self):

        info = self.get_context_info()
        r = info.create_renderable()
        mime_bundle = r._repr_mimebundle_(include=[], exclude=[])  # type: ignore
        return mime_bundle["text/html"]
