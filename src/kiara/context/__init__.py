# -*- coding: utf-8 -*-
import atexit
import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Set, Type, Union

import structlog

# from alembic import command  # type: ignore
from pydantic import Field

from kiara.context.config import KiaraArchiveConfig, KiaraConfig, KiaraContextConfig
from kiara.context.runtime_config import KiaraRuntimeConfig
from kiara.data_types import DataType
from kiara.exceptions import KiaraContextException
from kiara.interfaces import get_console
from kiara.interfaces.python_api.models.info import (
    DataTypeClassesInfo,
    InfoItemGroup,
    ItemInfo,
    KiaraModelClassesInfo,
    ModuleTypeInfo,
    ModuleTypesInfo,
    OperationGroupInfo,
    OperationTypeClassesInfo,
)
from kiara.interfaces.python_api.value import StoreValueResult, StoreValuesResult
from kiara.models import KiaraModel
from kiara.models.context import ContextInfo
from kiara.models.module.manifest import Manifest
from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.value import ValueMap
from kiara.registries import KiaraArchive, SqliteArchiveConfig
from kiara.registries.aliases import AliasRegistry
from kiara.registries.data import DataRegistry
from kiara.registries.environment import EnvironmentRegistry
from kiara.registries.events.metadata import CreateMetadataDestinies
from kiara.registries.events.registry import EventRegistry
from kiara.registries.ids import ID_REGISTRY
from kiara.registries.jobs import JobRegistry
from kiara.registries.metadata import MetadataRegistry
from kiara.registries.models import ModelRegistry
from kiara.registries.modules import ModuleRegistry
from kiara.registries.operations import OperationRegistry
from kiara.registries.rendering import RenderRegistry
from kiara.registries.types import TypeRegistry
from kiara.registries.workflows import WorkflowRegistry
from kiara.utils import log_exception, log_message
from kiara.utils.class_loading import find_all_archive_types
from kiara.utils.operations import filter_operations
from kiara.utils.stores import check_external_archive

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


if TYPE_CHECKING:
    from kiara.modules import KiaraModule


logger = structlog.getLogger()


def explain(item: Any, kiara: Union[None, "Kiara"] = None):
    """Pretty print information about an item on the terminal."""
    if isinstance(item, type):
        from kiara.modules import KiaraModule

        if issubclass(item, KiaraModule):
            if kiara is None:
                kiara = Kiara.instance()
            item = ModuleTypeInfo.create_from_type_class(type_cls=item, kiara=kiara)

    console = get_console()
    console.print(item)


class Kiara(object):
    """
    The core context of a kiara session.

    The `Kiara` object holds all information related to the current environment the user does works in. This includes:

      - available modules, operations & pipelines
      - available value data_types
      - available metadata schemas
      - available data items
      - available controller and processor data_types
      - misc. configuration options

    It's possible to use *kiara* without ever manually touching the 'Kiara' class, by default all relevant classes and functions
    will use a default instance of this class (available via the `Kiara.instance()` method.

    The Kiara class is highly dependent on the Python environment it lives in, because it auto-discovers available sub-classes
    of its building blocks (modules, value data_types, etc.). So, you can't assume that, for example, a pipeline you create
    will work the same way (or at all) in a different environment. *kiara* will always be able to tell you all the details
    of this environment, though, and it will attach those details to things like data, so there is always a record of
    how something was created, and in which environment.
    """

    @classmethod
    def instance(cls) -> "Kiara":
        """The default *kiara* context. In most cases, it's recommended you create and manage your own, though."""

        raise NotImplementedError("Kiara.instance() is not implemented yet.")
        # return BaseAPI.instance().context

    def __init__(
        self,
        config: Union[KiaraContextConfig, None] = None,
        runtime_config: Union[KiaraRuntimeConfig, None] = None,
    ) -> None:

        kc: Union[KiaraConfig, None] = None
        if not config:
            kc = KiaraConfig()
            config = kc.get_context_config()

        if not runtime_config:
            if kc is None:
                kc = KiaraConfig()
            runtime_config = kc.runtime_config

        self._id: uuid.UUID = ID_REGISTRY.generate(
            id=uuid.UUID(config.context_id), obj=self
        )
        ID_REGISTRY.update_metadata(self._id, kiara_id=self._id)
        self._config: KiaraContextConfig = config
        self._runtime_config: KiaraRuntimeConfig = runtime_config

        self._env_mgmt: EnvironmentRegistry = EnvironmentRegistry()

        self._event_registry: EventRegistry = EventRegistry(kiara=self)
        self._type_registry: TypeRegistry = TypeRegistry(self)
        self._data_registry: DataRegistry = DataRegistry(kiara=self)
        self._metadata_registry: MetadataRegistry = MetadataRegistry(kiara=self)
        self._job_registry: JobRegistry = JobRegistry(kiara=self)
        self._module_registry: ModuleRegistry = ModuleRegistry(kiara=self)
        self._operation_registry: OperationRegistry = OperationRegistry(kiara=self)

        self._kiara_model_registry: ModelRegistry = ModelRegistry.instance()

        self._alias_registry: AliasRegistry = AliasRegistry(kiara=self)
        # self._destiny_registry: DestinyRegistry = DestinyRegistry(kiara=self)

        self._workflow_registry: WorkflowRegistry = WorkflowRegistry(kiara=self)

        self._render_registry = RenderRegistry(kiara=self)

        metadata_augmenter = CreateMetadataDestinies(kiara=self)
        self._event_registry.add_listener(
            metadata_augmenter, *metadata_augmenter.supported_event_types()
        )

        self._context_info: Union[KiaraContextInfo, None] = None

        # initialize stores
        self._archive_types = find_all_archive_types()
        self._archives: Dict[str, KiaraArchive] = {}

        for archive_alias, archive in self._config.archives.items():

            # TODO: this is just to make old context that still had that not error out
            if "_destiny_" in archive.archive_type:
                continue

            if (
                archive_alias == "default_job_store"
                and archive.archive_type == "filesystem_job_store"
            ):

                # this is a temporary solution for contexts that still have the old filesystem job store
                # TODO: remove this at some stage

                archive_path = Path(archive.config["archive_path"])
                file_name = f"{archive_path.name}.kiarchive"

                js_config = SqliteArchiveConfig.create_new_store_config(
                    store_base_path=archive.config["archive_path"],
                    file_name=file_name,
                    use_wal_mode=True,
                )
                archive = KiaraArchiveConfig(
                    archive_type="sqlite_job_store", config=js_config.model_dump()
                )

            archive_cls = self._archive_types.get(archive.archive_type, None)

            if archive_cls is None:
                raise Exception(
                    f"Can't create context: no archive type '{archive.archive_type}' available. Available types: {', '.join(self._archive_types.keys())}"
                )

            config_cls = archive_cls._config_cls
            archive_config = config_cls(**archive.config)
            archive_obj = archive_cls(archive_name=archive_alias, archive_config=archive_config)  # type: ignore
            for supported_type in archive_obj.supported_item_types():
                if supported_type == "metadata":
                    self.metadata_registry.register_metadata_archive(archive_obj)  # type: ignore
                if supported_type == "data":
                    self.data_registry.register_data_archive(
                        archive_obj,  # type: ignore
                    )
                if supported_type == "job_record":
                    self.job_registry.register_job_archive(archive_obj)  # type: ignore

                if supported_type == "alias":
                    self.alias_registry.register_archive(archive_obj)  # type: ignore

                # if supported_type == "destiny":
                #     self.destiny_registry.register_destiny_archive(archive_obj)  # type: ignore

                if supported_type == "workflow":
                    self.workflow_registry.register_archive(archive_obj)  # type: ignore

        if self._runtime_config.lock_context:
            self.lock_context()

    def lock_context(self):
        """Lock the context, so that it can't be used by other processes."""
        aquired = ID_REGISTRY.lock_context(self.id)

        if not aquired:
            raise KiaraContextException(
                "Can't lock context: already locked by another process.",
                context_id=self.id,
            )

        atexit.register(self.unlock_context)

    def unlock_context(self):

        ID_REGISTRY.unlock_context(self.id)

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def context_config(self) -> KiaraContextConfig:
        return self._config

    @property
    def runtime_config(self) -> KiaraRuntimeConfig:
        return self._runtime_config

    def update_runtime_config(self, **settings) -> KiaraRuntimeConfig:

        for k, v in settings.items():
            setattr(self.runtime_config, k, v)

        return self.runtime_config

    @property
    def context_info(self) -> "KiaraContextInfo":

        if self._context_info is None:
            self._context_info = KiaraContextInfo.create_from_kiara_instance(kiara=self)
        return self._context_info

    # ===================================================================================================
    # registry accessors

    @property
    def environment_registry(self) -> EnvironmentRegistry:

        return self._env_mgmt

    @property
    def type_registry(self) -> TypeRegistry:
        return self._type_registry

    @property
    def module_registry(self) -> ModuleRegistry:
        return self._module_registry

    @property
    def kiara_model_registry(self) -> ModelRegistry:
        return self._kiara_model_registry

    @property
    def alias_registry(self) -> AliasRegistry:
        return self._alias_registry

    # @property
    # def destiny_registry(self) -> DestinyRegistry:
    #     return self._destiny_registry

    @property
    def job_registry(self) -> JobRegistry:
        return self._job_registry

    @property
    def operation_registry(self) -> OperationRegistry:
        op_registry = self._operation_registry
        return op_registry

    @property
    def data_registry(self) -> DataRegistry:
        return self._data_registry

    @property
    def metadata_registry(self) -> MetadataRegistry:
        return self._metadata_registry

    @property
    def workflow_registry(self) -> WorkflowRegistry:
        return self._workflow_registry

    @property
    def event_registry(self) -> EventRegistry:
        return self._event_registry

    @property
    def render_registry(self) -> RenderRegistry:
        return self._render_registry

    # ===================================================================================================
    # context specific types & instances

    @property
    def current_environments(self) -> Mapping[str, RuntimeEnvironment]:
        return self.environment_registry.environments

    @property
    def data_type_classes(self) -> Mapping[str, Type[DataType]]:
        return self.type_registry.data_type_classes

    @property
    def data_type_names(self) -> List[str]:
        return self.type_registry.get_data_type_names(include_profiles=True)

    @property
    def module_type_classes(self) -> Mapping[str, Type["KiaraModule"]]:
        return self._module_registry.module_types

    @property
    def module_type_names(self) -> Iterable[str]:
        return self._module_registry.get_module_type_names()

    # ===================================================================================================
    # kiara session API methods

    def register_external_archive(
        self,
        archive: Union[str, KiaraArchive, Iterable[Union[KiaraArchive, str]]],
        allow_write_access: bool = False,
    ) -> Dict[str, str]:
        """Register one or several external archives with the context.

        In case you provide KiaraArchive instances, they will be modified in case the provided 'allow_write_access' is different from the 'is_force_read_only' attribute of the archive.
        """

        archive_instances = check_external_archive(
            archive=archive, allow_write_access=allow_write_access
        )

        result = {}
        for archive_type, _archive_inst in archive_instances.items():
            log_message(
                "register.external.archive",
                archive=_archive_inst.archive_name,
                allow_write_access=allow_write_access,
            )

            _archive_inst.set_force_read_only(not allow_write_access)

            if archive_type == "data":
                result["data"] = self.data_registry.register_data_archive(_archive_inst)  # type: ignore
                log_message(
                    "archive.registered",
                    archive=_archive_inst.archive_name,
                    archive_type="data",
                )
            elif archive_type == "metadata":
                result["metadata"] = self.metadata_registry.register_metadata_archive(_archive_inst)  # type: ignore
                log_message(
                    "archive.registered",
                    archive=_archive_inst.archive_name,
                    archive_type="metadata",
                )
            elif archive_type == "alias":
                result["alias"] = self.alias_registry.register_archive(_archive_inst)  # type: ignore
                log_message(
                    "archive.registered",
                    archive=_archive_inst.archive_name,
                    archive_type="alias",
                )
            elif archive_type == "job_record":
                result["job_record"] = self.job_registry.register_job_archive(_archive_inst)  # type: ignore
                log_message(
                    "archive.registered",
                    archive=_archive_inst.archive_name,
                    archive_type="job_record",
                )
            else:
                raise Exception(f"Can't register archive of type '{archive_type}'.")

        return result

    def create_manifest(
        self, module_or_operation: str, config: Union[Mapping[str, Any], None] = None
    ) -> Manifest:

        if config is None:
            config = {}

        if module_or_operation in self.module_type_names:

            manifest: Manifest = Manifest(
                module_type=module_or_operation, module_config=config
            )

        elif module_or_operation in self.operation_registry.operation_ids:

            if config:
                raise Exception(
                    f"Specified run target '{module_or_operation}' is an operation, additional module configuration is not allowed (yet)."
                )
            manifest = self.operation_registry.get_operation(module_or_operation)

        elif os.path.isfile(module_or_operation):
            raise NotImplementedError()

        else:
            raise Exception(
                f"Can't assemble operation, invalid operation/module name: {module_or_operation}. Must be registered module or operation name, or file."
            )

        return manifest

    # def create_module(self, manifest: Union[Manifest, str]) -> "KiaraModule":
    #     """Create a [KiaraModule][kiara.module.KiaraModule] object from a module configuration.
    #
    #     Arguments:
    #         manifest: the module configuration
    #     """
    #
    #     return self._module_registry.create_module(manifest=manifest)

    def queue(
        self, manifest: Manifest, inputs: Mapping[str, Any], wait: bool = False
    ) -> uuid.UUID:
        """
        Queue a job with the specified manifest and inputs.

        Arguments:
        ---------
           manifest: the job manifest
           inputs: the job inputs
           wait: whether to wait for the job to be finished before returning

        Returns:
        -------
            the job id that can be used to look up job status & results
        """
        return self.job_registry.execute(manifest=manifest, inputs=inputs, wait=wait)

    def process(self, manifest: Manifest, inputs: Mapping[str, Any]) -> ValueMap:
        """
        Queue a job with the specified manifest and inputs.

        Arguments:
        ---------
           manifest: the job manifest
           inputs: the job inputs
           wait: whether to wait for the job to be finished before returning

        Returns:
        -------
        """
        return self.job_registry.execute_and_retrieve(manifest=manifest, inputs=inputs)

    def save_values(
        self, values: ValueMap, alias_map: Mapping[str, Iterable[str]]
    ) -> StoreValuesResult:

        _values = {}
        for field_name in values.field_names:
            value = values.get_value_obj(field_name)
            _values[field_name] = value
            self.data_registry.store_value(value=value)
        stored = {}
        for field_name, field_aliases in alias_map.items():

            value = _values[field_name]
            try:
                if field_aliases:
                    self.alias_registry.register_aliases(
                        value_id=value.value_id, aliases=field_aliases
                    )

                stored[field_name] = StoreValueResult(
                    value=value,
                    aliases=sorted(field_aliases),
                    error=None,
                    persisted_data=None,
                )

            except Exception as e:
                log_exception(e)
                stored[field_name] = StoreValueResult(
                    value=value,
                    aliases=sorted(field_aliases),
                    error=str(e),
                    persisted_data=None,
                )

        return StoreValuesResult(root=stored)

    def create_context_summary(self) -> ContextInfo:
        return ContextInfo.create_from_context(kiara=self)

    def get_all_archives(self) -> Dict[KiaraArchive, Set[str]]:

        result: Dict[KiaraArchive, Set[str]] = {}

        archive: KiaraArchive
        for alias, archive in self.metadata_registry.metadata_archives.items():
            result.setdefault(archive, set()).add(alias)
        for alias, archive in self.data_registry.data_archives.items():
            result.setdefault(archive, set()).add(alias)
        for alias, archive in self.alias_registry.alias_archives.items():
            result.setdefault(archive, set()).add(alias)
        # for alias, archive in self.destiny_registry.destiny_archives.items():
        #     result.setdefault(archive, set()).add(alias)
        for alias, archive in self.job_registry.job_archives.items():
            result.setdefault(archive, set()).add(alias)
        for alias, archive in self.workflow_registry.workflow_archives.items():
            result.setdefault(archive, set()).add(alias)

        return result


class KiaraContextInfo(KiaraModel):
    @classmethod
    def create_from_kiara_instance(
        cls, kiara: "Kiara", package_filter: Union[str, None] = None
    ):

        data_types = kiara.type_registry.get_context_metadata(
            only_for_package=package_filter
        )
        modules = kiara.module_registry.get_context_metadata(
            only_for_package=package_filter
        )
        operation_types = kiara.operation_registry.get_context_metadata(
            only_for_package=package_filter
        )
        operations = filter_operations(
            kiara=kiara, pkg_name=package_filter, **kiara.operation_registry.operations
        )

        model_registry = kiara.kiara_model_registry
        if package_filter:
            kiara_models = model_registry.get_models_for_package(
                package_name=package_filter
            )
        else:
            kiara_models = model_registry.all_models

        # metadata_types = find_metadata_models(only_for_package=package_filter)

        return KiaraContextInfo(
            kiara_id=kiara.id,
            package_filter=package_filter,
            data_types=data_types,
            module_types=modules,
            kiara_model_types=kiara_models,
            # metadata_types=metadata_types,
            operation_types=operation_types,
            operations=operations,
        )

    kiara_id: uuid.UUID = Field(description="The id of the kiara context.")
    package_filter: Union[str, None] = Field(
        description="Whether this context is filtered to only include information included in a specific Python package."
    )
    data_types: DataTypeClassesInfo = Field(description="The included data types.")
    module_types: ModuleTypesInfo = Field(
        description="The included kiara module types."
    )
    kiara_model_types: KiaraModelClassesInfo = Field(
        description="The included model classes."
    )
    # metadata_types: MetadataTypeClassesInfo = Field(
    #     description="The included value metadata types."
    # )
    operation_types: OperationTypeClassesInfo = Field(
        description="The included operation types."
    )
    operations: OperationGroupInfo = Field(description="The included operations.")

    def _retrieve_id(self) -> str:
        if not self.package_filter:
            return str(self.kiara_id)
        else:
            return f"{self.kiara_id}.package_{self.package_filter}"

    def _retrieve_data_to_hash(self) -> Any:
        return {"kiara_id": self.kiara_id, "package": self.package_filter}

    def get_info(self, item_type: str, item_id: str) -> ItemInfo:

        if item_type in ("data_type", "data_types"):
            group_info: InfoItemGroup = self.data_types
        elif "module" in item_type:
            group_info = self.module_types
        # elif "metadata" in item_type:
        #     group_info = self.metadata_types
        elif "operation_type" in item_type or "operation_types" in item_type:
            group_info = self.operation_types
        elif "operation" in item_type:
            group_info = self.operations
        elif "kiara_model" in item_type:
            group_info = self.kiara_model_types
        else:
            item_types = [
                "data_type",
                "module_type",
                "kiara_model_type",
                "operation_type",
                "operation",
            ]
            raise Exception(
                f"Can't determine item type '{item_type}', use one of: {', '.join(item_types)}"
            )
        result: ItemInfo = group_info.item_infos[item_id]
        return result

    def get_all_info(self, skip_empty_types: bool = True) -> Dict[str, InfoItemGroup]:

        result: Dict[str, InfoItemGroup] = {}
        if self.data_types or not skip_empty_types:
            result["data_types"] = self.data_types
        if self.module_types or not skip_empty_types:
            result["module_types"] = self.module_types
        if self.kiara_model_types or not skip_empty_types:
            result["kiara_model_types"] = self.kiara_model_types
        # if self.metadata_types or not skip_empty_types:
        #     result["metadata_types"] = self.metadata_types
        if self.operation_types or not skip_empty_types:
            result["operation_types"] = self.operation_types
        if self.operations or not skip_empty_types:
            result["operations"] = self.operations

        return result


# def delete_context(kiara_config: KiaraConfig, context_name: str):
#
#     kiara_context_config = kiara_config.get_context_config(context_name=context_name)
#     kiara = Kiara(config=kiara_context_config)
#
#     data_archives = kiara.data_registry.data_archives.values()
#     alias_archives = kiara.alias_registry.alias_archives.values()
#     job_archives = kiara.job_registry.job_archives.values()
#     destiny_archives = kiara.destiny_registry.destiny_archives.values()
#
#     clashes: Dict[str, List[KiaraArchive]] = {}
#     for context_name, context_config in kiara_config.context_configs.items():
#         k = Kiara(config=context_config)
#         for da in k.data_registry.data_archives.values():
#             if da in data_archives:
#                 clashes.setdefault("data", []).append(da)
#         for aa in k.alias_registry.alias_archives.values():
#             if aa in alias_archives:
#                 clashes.setdefault("alias", []).append(aa)
#         for ja in k.job_registry.job_archives.values():
#             if ja in job_archives:
#                 clashes.setdefault("job", []).append(ja)
#         for dea in k.destiny_registry.destiny_archives.values():
#             if dea in destiny_archives:
#                 clashes.setdefault("destiny", []).append(dea)
#
#     if clashes:
#         # TODO: only delete non-clash archives and don't throw exception
#         raise Exception(
#             f"Can't delete context '{context_name}', some archives are used in other contexts: {clashes}"
#         )
#
#     for da in data_archives:
#         da.delete_archive(archive_id=da.archive_id)
#
#     for aa in alias_archives:
#         aa.delete_archive(archive_id=aa.archive_id)
#
#     for ja in job_archives:
#         ja.delete_archive(archive_id=ja.archive_id)
#
#     for dea in destiny_archives:
#         dea.delete_archive(archive_id=dea.archive_id)
