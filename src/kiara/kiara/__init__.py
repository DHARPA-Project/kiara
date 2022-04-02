# -*- coding: utf-8 -*-
import os
import structlog
import uuid
from alembic import command
from sqlalchemy.engine import Engine, create_engine
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

from kiara.data_types import DataType
from kiara.defaults import KIARA_DB_MIGRATIONS_CONFIG, KIARA_DB_MIGRATIONS_FOLDER
from kiara.interfaces import get_console
from kiara.kiara.alias_registry import AliasRegistry
from kiara.kiara.config import KiaraContextConfig, KiaraGlobalConfig
from kiara.kiara.data_registry import DataRegistry
from kiara.kiara.environment_registry import EnvironmentRegistry
from kiara.kiara.job_registry import JobRegistry
from kiara.kiara.module_registry import ModuleRegistry
from kiara.kiara.operation_registry import OperationRegistry
from kiara.kiara.orm import EnvironmentOrm
from kiara.kiara.type_registry import TypeRegistry
from kiara.models.module import KiaraModuleTypeMetadata
from kiara.models.module.manifest import Manifest
from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.value import Value
from kiara.utils import is_debug, is_develop, log_message
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

        self._id: uuid.UUID = KIARA_IDS.generate(
            id=config.context_id, type="kiara context"
        )
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

        self._envs: Optional[Mapping[str, EnvironmentOrm]] = None
        self._type_registry: TypeRegistry = TypeRegistry(self)
        self._module_registry: ModuleRegistry = ModuleRegistry()
        self._operation_registry: OperationRegistry = OperationRegistry(kiara=self)
        self._data_registry: DataRegistry = DataRegistry(kiara=self)
        self._job_registry: JobRegistry = JobRegistry(kiara=self)
        self._alias_registry: AliasRegistry = AliasRegistry(kiara=self)

        self._env_mgmt: Optional[EnvironmentRegistry] = None

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
    def job_registry(self) -> JobRegistry:
        return self._job_registry

    @property
    def operation_registry(self) -> OperationRegistry:
        return self._operation_registry

    @property
    def data_registry(self) -> DataRegistry:
        return self._data_registry

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


class UUIDGenerator(object):
    def __init__(self):
        self._ids: Dict[uuid.UUID, Any] = {}

    def generate(self, id: Optional[uuid.UUID] = None, **metadata: Any):

        if id is None:
            id = uuid.uuid4()
        if is_debug() or is_develop():

            obj = metadata.pop("obj", None)
            # TODO: store this in a weakref dict
            logger.debug("generate.id", id=id, metadata=metadata)
            self._ids.setdefault(id, []).append(metadata)

        return id


KIARA_IDS = UUIDGenerator()