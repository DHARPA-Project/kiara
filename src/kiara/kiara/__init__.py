# -*- coding: utf-8 -*-
import os
import structlog
import uuid
from alembic import command
from sqlalchemy.engine import Engine, create_engine
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Type

from kiara.data_types import DataType
from kiara.data_types.type_mgmt import TypeMgmt
from kiara.defaults import KIARA_DB_MIGRATIONS_CONFIG, KIARA_DB_MIGRATIONS_FOLDER
from kiara.interfaces import get_console
from kiara.kiara.config import KiaraContextConfig, KiaraGlobalConfig
from kiara.kiara.data_registry import DataRegistry
from kiara.kiara.jobs import JobsMgmt
from kiara.kiara.orm import EnvironmentOrm
from kiara.models.module import KiaraModuleTypeMetadata
from kiara.models.module.manifest import Manifest
from kiara.models.runtime_environment import RuntimeEnvironment, RuntimeEnvironmentMgmt
from kiara.models.values.value import ValueSet
from kiara.modules.mgmt.merged import MergedModuleManager
from kiara.modules.operations import OperationsMgmt
from kiara.utils import is_debug, log_message
from kiara.utils.db import orm_json_deserialize, orm_json_serialize

if TYPE_CHECKING:
    from kiara.modules import KiaraModule


logging = structlog.getLogger()


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
    def instance(cls):
        """The default *kiara* context. In most cases, it's recommended you create and manage your own, though."""

        if cls._instance is None:
            cls._instance = Kiara()
        return cls._instance

    def __init__(self, config: Optional[KiaraContextConfig] = None):

        if not config:
            kc = KiaraGlobalConfig()
            config = kc.get_context()

        self._id: uuid.UUID = uuid.UUID(config.context_id)
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
        self._type_mgmt_obj: TypeMgmt = TypeMgmt(self)
        self._module_mgr: MergedModuleManager = MergedModuleManager(
            config.module_managers,
            extra_pipeline_folders=self._config.extra_pipeline_folders,
            ignore_errors=self._config.ignore_errors,
        )
        self._operations_mgmt: OperationsMgmt = OperationsMgmt(kiara=self)

        self._data_registry: DataRegistry = DataRegistry(kiara=self)
        # self._persistence_mgmt: PersistenceMgmt = PersistenceMgmt(kiara=self)

        self._jobs_mgmt: JobsMgmt = JobsMgmt(kiara=self)
        self._cached_modules: Dict[str, Dict[int, KiaraModule]] = {}

        self._env_mgmt: Optional[RuntimeEnvironmentMgmt] = None

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
    def runtime_env_mgmt(self) -> RuntimeEnvironmentMgmt:
        if self._env_mgmt is not None:
            return self._env_mgmt

        self._env_mgmt = RuntimeEnvironmentMgmt.instance()
        return self._env_mgmt

    @property
    def type_mgmt(self) -> TypeMgmt:
        return self._type_mgmt_obj

    @property
    def module_mgmt(self) -> MergedModuleManager:
        return self._module_mgr

    @property
    def jobs_mgmt(self) -> JobsMgmt:
        return self._jobs_mgmt

    @property
    def operations_mgmt(self) -> OperationsMgmt:
        return self._operations_mgmt

    @property
    def data_registry(self) -> DataRegistry:
        return self._data_registry

    @property
    def environments(self) -> Mapping[str, RuntimeEnvironment]:

        return self.runtime_env_mgmt.environments

    @property
    def data_type_classes(self) -> Mapping[str, Type[DataType]]:
        return self.type_mgmt.data_type_classes

    @property
    def data_type_names(self) -> List[str]:
        return self.type_mgmt.data_type_names

    def get_data_type(
        self, data_type_name: str, data_type_config: Optional[Mapping[str, Any]] = None
    ) -> DataType:
        vt = self._type_mgmt_obj.retrieve_data_type(
            data_type_name=data_type_name, data_type_config=data_type_config
        )
        return vt

    @property
    def module_types(self) -> Mapping[str, Type["KiaraModule"]]:
        return self._module_mgr.module_types

    @property
    def module_type_names(self) -> List[str]:
        return sorted(self.module_types.keys())

    def get_module_class(self, module_type: str) -> Type["KiaraModule"]:
        return self._module_mgr.get_module_class(module_type=module_type)

    def create_module(self, manifest: Manifest) -> "KiaraModule":
        """Create a [KiaraModule][kiara.module.KiaraModule] object from a module configuration.

        Arguments:
            manifest: the module configuration
        """

        if self._cached_modules.setdefault(manifest.module_type, {}).get(
            manifest.manifest_hash, None
        ):
            return self._cached_modules[manifest.module_type][manifest.manifest_hash]

        m_cls: Type[KiaraModule] = self._module_mgr.get_module_class(
            manifest.module_type
        )
        m_hash = m_cls._calculate_module_hash(manifest.module_config)

        kiara_module = m_cls(module_config=manifest.module_config)
        assert (
            kiara_module.module_instance_hash == m_hash
        )  # TODO: might not be necessary? Leaving it in here for now, to see if it triggers at any stage.
        return kiara_module

    def execute(self, manifest: Manifest, inputs: Mapping[str, Any]) -> ValueSet:

        result = self._jobs_mgmt.execute(manifest=manifest, inputs=inputs)
        return result

    # def persist_value(self, value: Union[str, uuid.UUID, Value]):
    #
    #     value = self.data_registry.get_value(value)
    #     self.persistence_mgmt.persist_value(value=value)
