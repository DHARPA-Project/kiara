# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import structlog
import uuid
from alembic import command  # type: ignore
from pydantic import Field
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Type,
    Union,
)

from kiara.context.config import KiaraConfig, KiaraContextConfig
from kiara.data_types import DataType
from kiara.defaults import (
    CONTEXT_INFO_CATEGORY_ID,
    KIARA_DB_MIGRATIONS_CONFIG,
    KIARA_DB_MIGRATIONS_FOLDER,
)
from kiara.interfaces import get_console
from kiara.interfaces.python_api import StoreValueResult, StoreValuesResult
from kiara.models import KiaraModel
from kiara.models.info import InfoModelGroup, KiaraInfoModel
from kiara.models.module import KiaraModuleTypeInfo, ModuleTypeClassesInfo
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import OperationGroupInfo, OperationTypeClassesInfo
from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.data_type import DataTypeClassesInfo
from kiara.models.values.value import ValueMap
from kiara.models.values.value_metadata import MetadataTypeClassesInfo
from kiara.registries import KiaraArchive
from kiara.registries.aliases import AliasRegistry
from kiara.registries.data import DataRegistry
from kiara.registries.destinies.registry import DestinyRegistry
from kiara.registries.environment import EnvironmentRegistry
from kiara.registries.events.metadata import CreateMetadataDestinies
from kiara.registries.events.registry import EventRegistry
from kiara.registries.ids import ID_REGISTRY
from kiara.registries.jobs import JobRegistry
from kiara.registries.modules import ModuleRegistry
from kiara.registries.operations import OperationRegistry
from kiara.registries.types import TypeRegistry
from kiara.utils import is_debug, log_message
from kiara.utils.class_loading import find_all_archive_types
from kiara.utils.metadata import find_metadata_models
from kiara.utils.operations import filter_operations

if TYPE_CHECKING:
    from kiara.modules import KiaraModule


logger = structlog.getLogger()


def explain(item: Any):
    """Pretty print information about an item on the terminal."""

    if isinstance(item, type):
        from kiara.modules import KiaraModule

        if issubclass(item, KiaraModule):
            item = KiaraModuleTypeInfo.create_from_type_class(type_cls=item)

    console = get_console()
    console.print(item)


class Kiara(object):
    """The core context of a kiara session.

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

    _instance = None

    @classmethod
    def instance(cls) -> "Kiara":
        """The default *kiara* context. In most cases, it's recommended you create and manage your own, though."""

        if cls._instance is None:
            cls._instance = Kiara()
        return cls._instance

    def __init__(self, config: Optional[KiaraContextConfig] = None):

        if not config:
            kc = KiaraConfig()
            config = kc.get_context_config()

        self._id: uuid.UUID = ID_REGISTRY.generate(
            id=uuid.UUID(config.context_id), obj=self
        )
        ID_REGISTRY.update_metadata(self._id, kiara_id=self._id)
        self._config: KiaraContextConfig = config

        # if is_debug():
        #     echo = True
        # else:
        #     echo = False
        # self._engine: Engine = create_engine(
        #     self._config.db_url,
        #     echo=echo,
        #     future=True,
        #     json_serializer=orm_json_serialize,
        #     json_deserializer=orm_json_deserialize,
        # )

        # self._run_alembic_migrations()
        # self._envs: Optional[Mapping[str, EnvironmentOrm]] = None

        self._event_registry: EventRegistry = EventRegistry(kiara=self)
        self._type_registry: TypeRegistry = TypeRegistry(self)
        self._data_registry: DataRegistry = DataRegistry(kiara=self)
        self._job_registry: JobRegistry = JobRegistry(kiara=self)
        self._module_registry: ModuleRegistry = ModuleRegistry()
        self._operation_registry: OperationRegistry = OperationRegistry(kiara=self)

        self._alias_registry: AliasRegistry = AliasRegistry(kiara=self)
        self._destiny_registry: DestinyRegistry = DestinyRegistry(kiara=self)

        self._env_mgmt: Optional[EnvironmentRegistry] = None

        metadata_augmenter = CreateMetadataDestinies(kiara=self)
        self._event_registry.add_listener(
            metadata_augmenter, *metadata_augmenter.supported_event_types()
        )

        self._context_info: Optional[KiaraContextInfo] = None

        # initialize stores
        self._archive_types = find_all_archive_types()
        self._archives: Dict[str, KiaraArchive] = {}

        for archive_alias, archive in self._config.archives.items():
            archive_cls = self._archive_types.get(archive.archive_type, None)
            if archive_cls is None:
                raise Exception(
                    f"Can't create context: no archive type '{archive.archive_type}' available. Available types: {', '.join(self._archive_types.keys())}"
                )

            config_cls = archive_cls._config_cls
            archive_config = config_cls(**archive.config)
            archive_obj = archive_cls(archive_id=archive.archive_uuid, config=archive_config)  # type: ignore
            for supported_type in archive_obj.supported_item_types():
                if supported_type == "data":
                    self.data_registry.register_data_archive(
                        archive_obj, alias=archive_alias  # type: ignore
                    )
                if supported_type == "job_record":
                    self.job_registry.register_job_archive(archive_obj, alias=archive_alias)  # type: ignore

                if supported_type == "alias":
                    self.alias_registry.register_archive(archive_obj, alias=archive_alias)  # type: ignore

    def _run_alembic_migrations(self):
        script_location = os.path.abspath(KIARA_DB_MIGRATIONS_FOLDER)
        dsn = self._config.db_url
        log_message("running migration script", script=script_location, db_url=dsn)
        from alembic.config import Config

        alembic_cfg = Config(KIARA_DB_MIGRATIONS_CONFIG)
        alembic_cfg.set_main_option("script_location", script_location)
        alembic_cfg.set_main_option("sqlalchemy.url", dsn)
        command.upgrade(alembic_cfg, "head")

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def context_config(self) -> KiaraContextConfig:
        return self._config

    @property
    def context_info(self) -> "KiaraContextInfo":

        if self._context_info is None:
            self._context_info = KiaraContextInfo.create_from_kiara_instance(kiara=self)
        return self._context_info

    # ===================================================================================================
    # registry accessors

    @property
    def environment_registry(self) -> EnvironmentRegistry:
        if self._env_mgmt is not None:
            return self._env_mgmt

        self._env_mgmt = EnvironmentRegistry.instance()
        return self._env_mgmt

    @property
    def type_registry(self) -> TypeRegistry:
        return self._type_registry

    @property
    def module_registry(self) -> ModuleRegistry:
        return self._module_registry

    @property
    def alias_registry(self) -> AliasRegistry:
        return self._alias_registry

    @property
    def destiny_registry(self) -> DestinyRegistry:
        return self._destiny_registry

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
    def event_registry(self) -> EventRegistry:
        return self._event_registry

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
        return self.type_registry.data_type_names

    @property
    def module_type_classes(self) -> Mapping[str, Type["KiaraModule"]]:
        return self._module_registry.module_types

    @property
    def module_type_names(self) -> Iterable[str]:
        return self._module_registry.get_module_type_names()

    # ===================================================================================================
    # kiara session API methods

    def create_manifest(
        self, module_or_operation: str, config: Optional[Mapping[str, Any]] = None
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

    def create_module(self, manifest: Union[Manifest, str]) -> "KiaraModule":
        """Create a [KiaraModule][kiara.module.KiaraModule] object from a module configuration.

        Arguments:
            manifest: the module configuration
        """

        return self._module_registry.create_module(manifest=manifest)

    def queue(
        self, manifest: Manifest, inputs: Mapping[str, Any], wait: bool = False
    ) -> uuid.UUID:
        """Queue a job with the specified manifest and inputs.

        Arguments:
           manifest: the job manifest
           inputs: the job inputs
           wait: whether to wait for the job to be finished before returning

        Returns:
            the job id that can be used to look up job status & results
        """

        return self.job_registry.execute(manifest=manifest, inputs=inputs, wait=wait)

    def process(self, manifest: Manifest, inputs: Mapping[str, Any]) -> ValueMap:
        """Queue a job with the specified manifest and inputs.

        Arguments:
           manifest: the job manifest
           inputs: the job inputs
           wait: whether to wait for the job to be finished before returning

        Returns
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
                    self.alias_registry.register_aliases(value.value_id, *field_aliases)

                stored[field_name] = StoreValueResult.construct(
                    value=value, aliases=sorted(field_aliases), error=None
                )

            except Exception as e:
                if is_debug():
                    import traceback

                    traceback.print_exc()
                stored[field_name] = StoreValueResult.construct(
                    value=value, aliases=sorted(field_aliases), error=str(e)
                )

        return StoreValuesResult.construct(__root__=stored)

    # def run(self, module_or_operation: str, module_config: Mapping[str, Any] = None):
    #
    #     if isinstance(module_or_operation, str):
    #         if module_or_operation in self.operation_registry.operation_ids:
    #
    #             operation = self.operation_registry.get_operation(module_or_operation)
    #             if module_config:
    #                 print(
    #                     f"Specified run target '{module_or_operation}' is an operation, additional module configuration is not allowed."
    #                 )
    #
    #     elif module_or_operation in self.module_type_names:
    #
    #         if module_config is None:
    #             module_config = {}
    #         manifest = Manifest(
    #             module_type=module_or_operation, module_config=module_config
    #         )
    #
    #         module = self.create_module(manifest=manifest)
    #         operation = Operation.create_from_module(module)
    #
    #     elif os.path.isfile(module_or_operation):
    #         raise NotImplementedError()
    #         # module_name = kiara_obj.register_pipeline_description(
    #         #     module_or_operation, raise_exception=True
    #         # )
    #     else:
    #         merged = list(self.module_type_names)
    #         merged.extend(self.operation_registry.operation_ids)
    #         raise NoSuchExecutionTargetException(
    #             msg=f"Invalid run target name '[i]{module_or_operation}[/i]'. Must be a path to a pipeline file, or one of the available modules/operations.",
    #             available_targets=sorted(merged),
    #         )


class KiaraContextInfo(KiaraModel):
    @classmethod
    def create_from_kiara_instance(
        cls, kiara: "Kiara", package_filter: Optional[str] = None
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

        metadata_types = find_metadata_models(only_for_package=package_filter)

        return KiaraContextInfo.construct(
            kiara_id=kiara.id,
            package_filter=package_filter,
            data_types=data_types,
            module_types=modules,
            metadata_types=metadata_types,
            operation_types=operation_types,
            operations=operations,
        )

    kiara_id: uuid.UUID = Field(description="The id of the kiara context.")
    package_filter: Optional[str] = Field(
        description="Whether this context is filtered to only include information included in a specific Python package."
    )
    data_types: DataTypeClassesInfo = Field(description="The included data types.")
    module_types: ModuleTypeClassesInfo = Field(
        description="The included kiara module types."
    )
    metadata_types: MetadataTypeClassesInfo = Field(
        description="The included value metadata types."
    )
    operation_types: OperationTypeClassesInfo = Field(
        description="The included operation types."
    )
    operations: OperationGroupInfo = Field(description="The included operations.")

    def _retrieve_id(self) -> str:
        if not self.package_filter:
            return str(self.kiara_id)
        else:
            return f"{self.kiara_id}.package_{self.package_filter}"

    def _retrieve_category_id(self) -> str:
        return CONTEXT_INFO_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {"kiara_id": self.kiara_id, "package": self.package_filter}

    def get_info(self, item_type: str, item_id: str) -> KiaraInfoModel:

        if "data_type" == item_type or "data_types" == item_type:
            group_info: InfoModelGroup = self.data_types
        elif "module" in item_type:
            group_info = self.module_types
        elif "metadata" in item_type:
            group_info = self.metadata_types
        elif "operation_type" in item_type or "operation_types" in item_type:
            group_info = self.operation_types
        elif "operation" in item_type:
            group_info = self.operations
        else:
            item_types = [
                "data_type",
                "module_type",
                "metadata_type",
                "operation_type",
                "operation",
            ]
            raise Exception(
                f"Can't determine item type '{item_type}', use one of: {', '.join(item_types)}"
            )
        return group_info[item_id]

    def get_all_info(self, skip_empty_types: bool = True) -> Dict[str, InfoModelGroup]:

        result: Dict[str, InfoModelGroup] = {}
        if self.data_types or not skip_empty_types:
            result["data_types"] = self.data_types
        if self.module_types or not skip_empty_types:
            result["module_types"] = self.module_types
        if self.metadata_types or not skip_empty_types:
            result["metadata_types"] = self.metadata_types
        if self.operation_types or not skip_empty_types:
            result["operation_types"] = self.operation_types
        if self.operations or not skip_empty_types:
            result["operations"] = self.operations

        return result
