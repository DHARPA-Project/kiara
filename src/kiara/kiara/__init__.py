# -*- coding: utf-8 -*-
import os
import structlog
import uuid
from alembic import command
from sqlalchemy.engine import Engine, create_engine
from typing import TYPE_CHECKING, Any, Iterable, List, Mapping, Optional, Type, Union

from kiara.data_types import DataType
from kiara.defaults import KIARA_DB_MIGRATIONS_CONFIG, KIARA_DB_MIGRATIONS_FOLDER
from kiara.exceptions import NoSuchExecutionTargetException
from kiara.interfaces import get_console
from kiara.kiara.config import KiaraContextConfig, KiaraGlobalConfig
from kiara.models.module import KiaraModuleTypeMetadata
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import Operation
from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.value import Value
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
from kiara.utils.db import orm_json_deserialize, orm_json_serialize

if TYPE_CHECKING:
    from kiara.modules import KiaraModule


logger = structlog.getLogger()


def explain(item: Any):
    """Pretty print information about an item on the terminal."""

    if isinstance(item, type):
        from kiara.modules import KiaraModule

        if issubclass(item, KiaraModule):
            item = KiaraModuleTypeMetadata.from_module_class(module_cls=item)

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
            kc = KiaraGlobalConfig()
            config = kc.get_context()

        self._id: uuid.UUID = ID_REGISTRY.generate(id=config.context_id, obj=self)
        ID_REGISTRY.update_metadata(self._id, kiara_id=self._id)
        self._config: KiaraContextConfig = config

        if is_debug():
            echo = True
        else:
            echo = False
        self._engine: Engine = create_engine(
            self._config.db_url,
            echo=echo,
            future=True,
            json_serializer=orm_json_serialize,
            json_deserializer=orm_json_deserialize,
        )

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
        self._event_registry.add_listener(metadata_augmenter, *metadata_augmenter.supported_event_types())

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
        return self._operation_registry

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

    def create_module(self, manifest: Manifest) -> "KiaraModule":
        """Create a [KiaraModule][kiara.module.KiaraModule] object from a module configuration.

        Arguments:
            manifest: the module configuration
        """

        return self._module_registry.create_module(manifest=manifest)

    def get_value(self, value: Union[uuid.UUID, str, Value]):
        pass

    def run(self, module_or_operation: str, module_config: Mapping[str, Any] = None):

        if isinstance(module_or_operation, str):
            if module_or_operation in self.operation_registry.operation_ids:

                operation = self.operation_registry.get_operation(module_or_operation)
                if module_config:
                    print(
                        f"Specified run target '{module_or_operation}' is an operation, additional module configuration is not allowed."
                    )

        elif module_or_operation in self.module_type_names:

            if module_config is None:
                module_config = {}
            manifest = Manifest(
                module_type=module_or_operation, module_config=module_config
            )

            module = self.create_module(manifest=manifest)
            operation = Operation.create_from_module(module)

        elif os.path.isfile(module_or_operation):
            raise NotImplementedError()
            # module_name = kiara_obj.register_pipeline_description(
            #     module_or_operation, raise_exception=True
            # )
        else:
            merged = list(self.module_type_names)
            merged.extend(self.operation_registry.operation_ids)
            raise NoSuchExecutionTargetException(
                msg=f"Invalid run target name '[i]{module_or_operation}[/i]'. Must be a path to a pipeline file, or one of the available modules/operations.",
                available_targets=sorted(merged),
            )
