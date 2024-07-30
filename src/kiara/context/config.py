# -*- coding: utf-8 -*-
import contextlib

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
import uuid
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Type,
    Union,
)

import structlog
from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from pydantic.fields import Field, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict
from ruamel import yaml as r_yaml

from kiara.context.runtime_config import KiaraRuntimeConfig
from kiara.defaults import (
    DEFAULT_ALIAS_STORE_MARKER,
    DEFAULT_CONTEXT_NAME,
    DEFAULT_DATA_STORE_MARKER,
    DEFAULT_JOB_STORE_MARKER,
    DEFAULT_METADATA_STORE_MARKER,
    DEFAULT_WORKFLOW_STORE_MARKER,
    KIARA_CONFIG_FILE_NAME,
    KIARA_MAIN_CONFIG_FILE,
    KIARA_MAIN_CONTEXTS_PATH,
    kiara_app_dirs,
)
from kiara.exceptions import KiaraException
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import log_message
from kiara.utils.files import get_data_from_file

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.models.context import ContextInfo
    from kiara.registries import BaseArchive, KiaraArchive

logger = structlog.getLogger()

yaml = r_yaml.YAML(typ="safe", pure=True)
yaml.default_flow_style = False


def config_file_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    if os.path.isfile(KIARA_MAIN_CONFIG_FILE):
        config: Dict[str, Any] = get_data_from_file(
            KIARA_MAIN_CONFIG_FILE, content_type="yaml"
        )
        if not isinstance(config, Mapping):
            raise ValueError(
                f"Invalid config file format, can't parse file: {KIARA_MAIN_CONFIG_FILE}"
            )
    else:
        config = {}
    return config


class KiaraArchiveConfig(BaseModel):
    """Configuration data that can be used to load an existing kiara archive."""

    # archive_alias: str = Field(description="The unique archive id.")
    archive_type: str = Field(description="The archive type.")
    config: Mapping[str, Any] = Field(
        description="Archive type specific config.", default_factory=dict
    )


class KiaraArchiveReference(BaseModel):
    @classmethod
    def load_existing_archive(
        cls,
        archive_uri: str,
        store_type: Union[str, None, Iterable[str]] = None,
        allow_write_access: bool = False,
        archive_name: Union[str, None] = None,
        **kwargs: Any,
    ) -> "KiaraArchiveReference":

        from kiara.utils.class_loading import find_all_archive_types

        archive_types = find_all_archive_types()

        archive_configs: List[KiaraArchiveConfig] = []
        archives: List[KiaraArchive] = []

        if store_type:
            if isinstance(store_type, str):
                archive_cls: Union[Type[KiaraArchive], None] = archive_types.get(
                    store_type, None
                )
                if archive_cls is None:
                    raise Exception(
                        f"Can't create context: no archive type '{store_type}' available. Available types: {', '.join(archive_types.keys())}"
                    )
                data = archive_cls.load_archive_config(
                    archive_uri=archive_uri,
                    allow_write_access=allow_write_access,
                    **kwargs,
                )
                archive_config = archive_cls._config_cls(**data)
                archive: KiaraArchive = archive_cls(
                    archive_config=archive_config, archive_name=archive_name
                )
                wrapped_archive_config = KiaraArchiveConfig(
                    archive_type=store_type, config=data
                )
                archive_configs.append(wrapped_archive_config)
                archives.append(archive)
            else:
                for st in store_type:
                    archive_cls = archive_types.get(st, None)
                    if archive_cls is None:
                        raise Exception(
                            f"Can't create context: no archive type '{store_type}' available. Available types: {', '.join(archive_types.keys())}"
                        )
                    data = archive_cls.load_archive_config(
                        archive_uri=archive_uri,
                        allow_write_access=allow_write_access,
                        **kwargs,
                    )
                    archive_config = archive_cls._config_cls(**data)
                    archive = archive_cls(
                        archive_config=archive_config, archive_name=archive_name
                    )
                    wrapped_archive_config = KiaraArchiveConfig(
                        archive_type=st, config=data
                    )
                    archive_configs.append(wrapped_archive_config)
                    archives.append(archive)
        else:
            for archive_type, archive_cls in archive_types.items():
                data = archive_cls.load_archive_config(
                    archive_uri=archive_uri,
                    allow_write_access=allow_write_access,
                    **kwargs,
                )

                if data is None:
                    continue

                archive_config = archive_cls._config_cls(**data)
                archive = archive_cls(
                    archive_config=archive_config, archive_name=archive_name
                )
                wrapped_archive_config = KiaraArchiveConfig(
                    archive_type=archive_type, config=data
                )
                archive_configs.append(wrapped_archive_config)
                archives.append(archive)

        if archives is None:
            raise Exception(
                f"Can't create context: no valid archive found at '{archive_uri}'"
            )

        result = cls(
            archive_uri=archive_uri,
            allow_write_access=allow_write_access,
            archive_configs=archive_configs,
            # archive_alias=archive_alias,
        )
        result._archives = archives
        return result

    archive_uri: str = Field(description="The uri that points to the archive.")
    # archive_alias: str = Field(
    #     description="The alias that is used for the archives contained in here."
    # )
    allow_write_access: bool = Field(
        description="Whether to allow write access to the archives contained here.",
        default=False,
    )
    archive_configs: List[KiaraArchiveConfig] = Field(
        description="All the archives this kiara context can use and the aliases they are registered with."
    )
    _archives: Union[None, List["KiaraArchive"]] = PrivateAttr(default=None)

    @property
    def archives(self) -> List["KiaraArchive"]:

        if self._archives is not None:
            return self._archives

        from kiara.utils.class_loading import find_all_archive_types

        archive_types = find_all_archive_types()

        archive_alias = None

        result = []
        for config in self.archive_configs:
            if config.archive_type not in archive_types.keys():
                raise Exception(
                    f"Can't create context: no archive type '{config.archive_type}' available. Available types: {', '.join(archive_types.keys())}"
                )

            archive_cls = archive_types[config.archive_type]
            archive_config_data = archive_cls.load_archive_config(
                archive_uri=self.archive_uri,
                allow_write_access=self.allow_write_access,
            )
            archive_config = archive_cls._config_cls(**archive_config_data)
            archive = archive_cls(
                archive_config=archive_config, archive_name=archive_alias
            )
            result.append(archive)

        self._archives = result
        return self._archives


class KiaraContextConfig(BaseModel):
    @classmethod
    def create_from_sqlite_db(cls, db_path: Path) -> "KiaraContextConfig":

        import sqlite3

        if not db_path.exists():
            context_id = str(uuid.uuid4())
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute(
                """CREATE TABLE context_metadata
                         (key text PRIMARY KEY , value text NOT NULL)"""
            )
            c.execute(
                "INSERT INTO context_metadata VALUES ('context_id', ?)", (context_id,)
            )
            c.execute(
                """CREATE TABLE archive_metadata
                         (key text PRIMARY KEY , value text NOT NULL)"""
            )
            c.execute(
                "INSERT INTO archive_metadata VALUES ('archive_id', ?)", (context_id,)
            )

            conn.commit()
            conn.close()
        else:
            try:

                with sqlite3.connect(db_path) as conn:
                    context_id = conn.execute(
                        "SELECT value FROM context_metadata WHERE key = 'context_id'"
                    ).fetchone()[0]
            except Exception as e:
                raise KiaraException(
                    f"Can't read context from sqlite db '{db_path}': {e}"
                )

        base_path = os.path.abspath(kiara_app_dirs.user_data_dir)
        stores_base_path = os.path.join(base_path, "stores")
        workflow_base_path = os.path.join(
            stores_base_path, "filesystem_stores", "workflows"
        )
        workflow_store_path = os.path.join(workflow_base_path, context_id)

        data_store_config = KiaraArchiveConfig(
            archive_type="sqlite_data_store",
            config={"sqlite_db_path": db_path.as_posix()},
        )
        alias_store_config = KiaraArchiveConfig(
            archive_type="sqlite_alias_store",
            config={"sqlite_db_path": db_path.as_posix()},
        )
        job_store_config = KiaraArchiveConfig(
            archive_type="sqlite_job_store",
            config={"sqlite_db_path": db_path.as_posix()},
        )
        workflow_store_config = KiaraArchiveConfig(
            archive_type="filesystem_workflow_store",
            config={"archive_path": workflow_store_path},
        )
        metadata_store_config = KiaraArchiveConfig(
            archive_type="sqlite_metadata_store",
            config={"sqlite_db_path": db_path.as_posix()},
        )

        archives = {
            DEFAULT_DATA_STORE_MARKER: data_store_config,
            DEFAULT_ALIAS_STORE_MARKER: alias_store_config,
            DEFAULT_JOB_STORE_MARKER: job_store_config,
            DEFAULT_WORKFLOW_STORE_MARKER: workflow_store_config,
            DEFAULT_METADATA_STORE_MARKER: metadata_store_config,
        }

        context_config = cls(
            context_id=context_id,
            archives=archives,
        )

        return context_config

    model_config = ConfigDict(extra="forbid")

    context_id: str = Field(description="A globally unique id for this kiara context.")

    archives: Dict[str, KiaraArchiveConfig] = Field(
        description="All the archives this kiara context can use and the aliases they are registered with."
    )
    extra_pipelines: List[str] = Field(
        description="Paths to local folders that contain kiara pipelines.",
        default_factory=list,
    )
    _context_config_path: Union[Path, None] = PrivateAttr(default=None)

    def add_pipelines(self, *pipelines: str):

        for pipeline in pipelines:
            if os.path.exists(pipeline):
                self.extra_pipelines.append(pipeline)
            else:
                logger.info(
                    "ignore.pipeline", reason="path does not exist", path=pipeline
                )

    # def create_archive(
    #     self, archive_alias: str, allow_write_access: bool = False
    # ) -> "KiaraArchive":
    #     """Create the kiara archive with the specified alias.
    #
    #     Make sure you know what you are doing when setting 'allow_write_access' to True.
    #     """
    #
    #     store_config = self.archives[archive_alias]
    #     store = create_store(
    #         archive_id=store_config.archive_uuid,
    #         store_type=store_config.archive_type,
    #         store_config=store_config.config,
    #         allow_write_access=allow_write_access,
    #     )
    #     return store


class KiaraSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="forbid", validate_assignment=True, env_prefix="kiara_setting_"
    )

    syntax_highlight_background: str = Field(
        description="The background color for code blocks when rendering to terminal, Jupyter, etc.",
        default="default",
    )


KIARA_SETTINGS = KiaraSettings()


def create_default_store_config(
    store_type: str, stores_base_path: str, use_wal_mode: bool = False
) -> KiaraArchiveConfig:

    from kiara.utils.archives import find_archive_types

    # env_registry = EnvironmentRegistry.instance()
    # find_archive_types = find_archive_types()
    # kiara_types: "KiaraTypesRuntimeEnvironment" = env_registry.environments["kiara_types"]  # type: ignore
    available_archives = find_archive_types()

    assert store_type in available_archives.item_infos.keys()

    from kiara.models.archives import ArchiveTypeInfo

    archive_info: ArchiveTypeInfo = available_archives.item_infos[store_type]
    cls: Type[BaseArchive] = archive_info.python_class.get_class()  # type: ignore

    log_message(
        "create_new_store",
        stores_base_path=stores_base_path,
        store_type=cls.__name__,
    )

    config = cls._config_cls.create_new_store_config(
        store_base_path=stores_base_path, use_wal_mode=use_wal_mode
    )

    # store_id: uuid.UUID = config.get_archive_id()

    data_store = KiaraArchiveConfig(
        archive_type=store_type,
        config=config.model_dump(),
    )
    return data_store


DEFAULT_STORE_TYPE: Literal["sqlite"] = "sqlite"


class KiaraConfig(BaseSettings):

    model_config = SettingsConfigDict(
        env_prefix="kiara_", extra="forbid", use_enum_values=True
    )

    @classmethod
    def create_in_folder(cls, path: Union[Path, str]) -> "KiaraConfig":

        if isinstance(path, str):
            path = Path(path)
        path = path.absolute()
        if path.exists():
            raise Exception(
                f"Can't create new kiara config, path exists: {path.as_posix()}"
            )

        config = KiaraConfig(base_data_path=path.as_posix())
        config_file = path / KIARA_CONFIG_FILE_NAME

        config.save(config_file)

        return config

    @classmethod
    def load_from_file(cls, path: Union[Path, str, None] = None) -> "KiaraConfig":

        if path is None:
            path = Path(KIARA_MAIN_CONFIG_FILE)
        elif isinstance(path, str):
            path = Path(path)

        if not path.exists():
            raise Exception(
                f"Can't load kiara config, path does not exist: {path.as_posix()}"
            )

        if path.is_dir():
            path = path / KIARA_CONFIG_FILE_NAME
            if not path.exists():
                raise Exception(
                    f"Can't load kiara config, path does not exist: {path.as_posix()}"
                )

        with path.open("rt") as f:
            data = yaml.load(f)

        config = KiaraConfig(**data)
        config._config_path = path
        return config

    context_search_paths: List[str] = Field(
        description="The base path to look for contexts in.",
        default=[KIARA_MAIN_CONTEXTS_PATH],
    )
    base_data_path: str = Field(
        description="The base path to use for all data (unless otherwise specified.",
        default=kiara_app_dirs.user_data_dir,
    )
    stores_base_path: str = Field(
        description="The base path for the stores of this context."
    )
    default_context: str = Field(
        description="The name of the default context to use if none is provided.",
        default=DEFAULT_CONTEXT_NAME,
    )
    # default_store_type: Literal["sqlite", "filesystem"] = Field(
    #     description="The default store type to use when creating new stores.",
    #     default=DEFAULT_STORE_TYPE,
    # )
    auto_generate_contexts: bool = Field(
        description="Whether to auto-generate requested contexts if they don't exist yet.",
        default=True,
    )
    runtime_config: KiaraRuntimeConfig = Field(
        description="The kiara runtime config.", default_factory=KiaraRuntimeConfig
    )

    _contexts: Dict[uuid.UUID, "Kiara"] = PrivateAttr(default_factory=dict)
    _available_context_files: Dict[str, Path] = PrivateAttr(default=None)
    _context_data: Dict[str, KiaraContextConfig] = PrivateAttr(default_factory=dict)
    _config_path: Union[Path, None] = PrivateAttr(default=None)

    @field_validator("context_search_paths")
    @classmethod
    def validate_context_search_paths(cls, v):

        if not v or not v[0]:
            v = [KIARA_MAIN_CONTEXTS_PATH]

        return v

    @model_validator(mode="before")
    @classmethod
    def _set_paths(cls, values: Any):

        base_path = values.get("base_data_path", None)
        if not base_path:
            base_path = os.path.abspath(kiara_app_dirs.user_data_dir)
            values["base_data_path"] = base_path
        elif isinstance(base_path, Path):
            base_path = base_path.absolute().as_posix()
            values["base_data_path"] = base_path

        stores_base_path = values.get("stores_base_path", None)
        if not stores_base_path:
            stores_base_path = os.path.join(base_path, "stores")
            values["stores_base_path"] = stores_base_path

        context_search_paths = values.get("context_search_paths")
        if not context_search_paths:
            context_search_paths = [os.path.join(base_path, "contexts")]
            values["context_search_paths"] = context_search_paths

        return values

    @property
    def available_context_names(self) -> Iterable[str]:

        if self._available_context_files is not None:
            return self._available_context_files.keys()

        result = {}
        for search_path in self.context_search_paths:
            sp = Path(search_path)
            for path in sp.rglob("*.yaml"):
                rel_path = path.relative_to(sp)
                alias = rel_path.as_posix()[0:-5]
                alias = alias.replace(os.sep, ".")
                result[alias] = path
        self._available_context_files = result
        return self._available_context_files.keys()

    @property
    def context_configs(self) -> Mapping[str, KiaraContextConfig]:

        return {a: self.get_context_config(a) for a in self.available_context_names}

    def get_context_config(
        self,
        context_name: Union[str, None] = None,
        auto_generate: Union[bool, None] = None,
    ) -> KiaraContextConfig:

        if auto_generate is None:
            auto_generate = self.auto_generate_contexts

        if context_name is None:
            context_name = self.default_context

        if context_name not in self.available_context_names:
            if not auto_generate and not context_name == DEFAULT_CONTEXT_NAME:
                raise Exception(
                    f"No kiara context with name '{context_name}' available."
                )
            else:
                return self.create_context_config(context_alias=context_name)

        if context_name in self._context_data.keys():
            return self._context_data[context_name]

        context_file = self._available_context_files[context_name]
        context_data = get_data_from_file(context_file, content_type="yaml")

        if not context_data:
            raise KiaraException(
                f"Empty/corrupted context file '{context_file.as_posix()}': delete file and try again."
            )

        changed = False
        if "extra_pipeline_folders" in context_data.keys():
            epf = context_data.pop("extra_pipeline_folders")
            context_data.setdefault("extra_pipelines", []).extend(epf)
            changed = True

        context = KiaraContextConfig(**context_data)

        if not changed:
            changed = self._validate_context(context_config=context)

        if changed:
            logger.debug(
                "write.context_file",
                context_config_file=context_file.as_posix(),
                context_name=context_name,
                reason="context changed after validation",
            )
            context_file.parent.mkdir(parents=True, exist_ok=True)
            with open(context_file, "wt") as f:
                yaml.dump(context.model_dump(), f)

        context._context_config_path = context_file

        self._context_data[context_name] = context
        return context

    def _validate_context(self, context_config: KiaraContextConfig) -> bool:

        changed = False

        sqlite_base_path = os.path.join(self.stores_base_path, "sqlite_stores")
        filesystem_base_path = os.path.join(self.stores_base_path, "filesystem_stores")

        def create_default_sqlite_archive_config(use_wal_mode: bool) -> Dict[str, Any]:

            store_id = str(uuid.uuid4())
            file_name = f"{store_id}.karchive"
            archive_path = Path(
                os.path.abspath(os.path.join(sqlite_base_path, file_name))
            )

            if archive_path.exists():
                raise Exception(
                    f"Archive path '{archive_path.as_posix()}' already exists."
                )

            archive_path.parent.mkdir(exist_ok=True, parents=True)

            # Connect to the SQLite database (or create it if it doesn't exist)
            import sqlite3

            conn = sqlite3.connect(archive_path)

            # Create a cursor object
            c = conn.cursor()
            # Create table
            c.execute(
                """CREATE TABLE archive_metadata
                         (key text PRIMARY KEY , value text NOT NULL)"""
            )
            c.execute(
                "INSERT INTO archive_metadata VALUES ('archive_id', ?)", (store_id,)
            )
            conn.commit()
            conn.close()

            return {
                "sqlite_db_path": archive_path.as_posix(),
                "use_wal_mode": use_wal_mode,
            }

        default_sqlite_config: Union[Dict[str, Any], None] = None

        use_wal_mode: bool = True
        default_store_type = "sqlite"

        if default_store_type == "auto":

            # if windows, we want sqlite as default, because although it's slower, it does not
            # need the user to enable developer mode
            if os.name == "nt":
                data_store_type = "sqlite"
            else:
                data_store_type = "filesystem"

            metadata_store_type = "sqlite"
            alias_store_type = "sqlite"
            job_store_type = "sqlite"
            workflow_store_type = "sqlite"
        elif default_store_type == "filesystem":
            metadata_store_type = "filesystem"
            data_store_type = "filesystem"
            alias_store_type = "filesystem"
            job_store_type = "filesystem"
            workflow_store_type = "filesystem"
        elif default_store_type == "sqlite":
            metadata_store_type = "sqlite"
            data_store_type = "sqlite"
            alias_store_type = "sqlite"
            job_store_type = "sqlite"
            workflow_store_type = "sqlite"
        else:
            raise Exception(f"Unknown store type: {default_store_type}")

        if DEFAULT_METADATA_STORE_MARKER not in context_config.archives.keys():

            if metadata_store_type == "sqlite":
                default_sqlite_config = create_default_sqlite_archive_config(
                    use_wal_mode=use_wal_mode
                )
                metaddata_store = KiaraArchiveConfig(
                    archive_type="sqlite_metadata_store", config=default_sqlite_config
                )
            elif metadata_store_type == "filesystem":
                default_sqlite_config = create_default_sqlite_archive_config(
                    use_wal_mode=use_wal_mode
                )
                metaddata_store = KiaraArchiveConfig(
                    archive_type="sqlite_metadata_store", config=default_sqlite_config
                )
            else:
                raise Exception(
                    f"Can't create default metadata store: invalid default store type '{metadata_store_type}'"
                )

            context_config.archives[DEFAULT_METADATA_STORE_MARKER] = metaddata_store
            changed = True

        if DEFAULT_DATA_STORE_MARKER not in context_config.archives.keys():

            if data_store_type == "sqlite":
                if default_sqlite_config is None:
                    default_sqlite_config = create_default_sqlite_archive_config(
                        use_wal_mode=use_wal_mode
                    )

                data_store = KiaraArchiveConfig(
                    archive_type="sqlite_data_store", config=default_sqlite_config
                )
            elif data_store_type == "filesystem":
                data_store_type = "filesystem_data_store"
                data_store = create_default_store_config(
                    store_type=data_store_type,
                    stores_base_path=os.path.join(filesystem_base_path, "data"),
                )
            else:
                raise Exception(
                    f"Can't create default data store: invalid default store type '{data_store_type}'."
                )

            context_config.archives[DEFAULT_DATA_STORE_MARKER] = data_store
            changed = True

        if DEFAULT_JOB_STORE_MARKER not in context_config.archives.keys():

            if job_store_type == "sqlite":

                if default_sqlite_config is None:
                    default_sqlite_config = create_default_sqlite_archive_config(
                        use_wal_mode=use_wal_mode
                    )

                job_store = KiaraArchiveConfig(
                    archive_type="sqlite_job_store", config=default_sqlite_config
                )
            elif job_store_type == "filesystem":
                job_store_type = "filesystem_job_store"
                job_store = create_default_store_config(
                    store_type=job_store_type,
                    stores_base_path=os.path.join(filesystem_base_path, "jobs"),
                )
            else:
                raise Exception(
                    f"Can't create default job store: invalid default store type '{job_store_type}'."
                )

            context_config.archives[DEFAULT_JOB_STORE_MARKER] = job_store
            changed = True

        if DEFAULT_ALIAS_STORE_MARKER not in context_config.archives.keys():

            if alias_store_type == "sqlite":

                if default_sqlite_config is None:
                    default_sqlite_config = create_default_sqlite_archive_config(
                        use_wal_mode=use_wal_mode
                    )

                alias_store = KiaraArchiveConfig(
                    archive_type="sqlite_alias_store", config=default_sqlite_config
                )
            elif alias_store_type == "filesystem":
                alias_store_type = "filesystem_alias_store"
                alias_store = create_default_store_config(
                    store_type=alias_store_type,
                    stores_base_path=os.path.join(filesystem_base_path, "aliases"),
                )
            else:
                raise Exception(
                    f"Can't create default alias store: invalid default store type '{alias_store_type}'."
                )

            context_config.archives[DEFAULT_ALIAS_STORE_MARKER] = alias_store
            changed = True

        if DEFAULT_WORKFLOW_STORE_MARKER not in context_config.archives.keys():

            # TODO: impolement sqlite type, or remove workflows entirely

            workflow_store_type = "filesystem_workflow_store"
            # workflow_store_type = "sqlite_workflow_store"

            workflow_store = create_default_store_config(
                store_type=workflow_store_type,
                stores_base_path=os.path.join(filesystem_base_path, "workflows"),
            )
            context_config.archives[DEFAULT_WORKFLOW_STORE_MARKER] = workflow_store
            changed = True

        return changed

    def create_context_config(
        self, context_alias: Union[str, None] = None
    ) -> KiaraContextConfig:

        if not context_alias:
            context_alias = DEFAULT_CONTEXT_NAME

        if context_alias in self.available_context_names:
            raise Exception(
                f"Can't create kiara context '{context_alias}': context with that alias already registered."
            )

        if context_alias.endswith(".kontext"):
            context_db_file = Path(context_alias)
            context_config: KiaraContextConfig = (
                KiaraContextConfig.create_from_sqlite_db(db_path=context_db_file)
            )
            self._validate_context(context_config=context_config)
            context_config._context_config_path = context_db_file
        else:

            if os.path.sep in context_alias:
                raise Exception(
                    f"Can't create context with alias '{context_alias}': no special characters allowed."
                )

            context_file = (
                Path(os.path.join(self.context_search_paths[0]))
                / f"{context_alias}.yaml"
            )

            archives: Dict[str, KiaraArchiveConfig] = {}
            # create_default_archives(kiara_config=self)
            context_id = ID_REGISTRY.generate(
                obj_type=KiaraContextConfig,
                comment=f"new kiara context '{context_alias}'",
            )

            context_config = KiaraContextConfig(
                context_id=str(context_id), archives=archives, extra_pipelines=[]
            )

            self._validate_context(context_config=context_config)

            context_file.parent.mkdir(parents=True, exist_ok=True)
            with open(context_file, "wt") as f:
                yaml.dump(context_config.model_dump(), f)

            context_config._context_config_path = context_file
            self._available_context_files[context_alias] = context_file

        self._context_data[context_alias] = context_config

        return context_config

    def create_context(
        self,
        context: Union[None, str, uuid.UUID, Path] = None,
        extra_pipelines: Union[None, str, Iterable[str]] = None,
    ) -> "Kiara":

        if not context:
            context = self.default_context
        else:
            with contextlib.suppress(Exception):
                context = uuid.UUID(context)  # type: ignore

        if isinstance(context, str) and (
            os.path.exists(context) or context.endswith(".kontext")
        ):
            context = Path(os.path.abspath(context))

        if isinstance(context, Path):
            if context.name.endswith(".kontext"):
                context_config = KiaraContextConfig.create_from_sqlite_db(
                    db_path=context
                )
            else:
                try:
                    with context.open("rt") as f:
                        data = yaml.load(f)
                except Exception as e:
                    raise KiaraException(
                        f"Can't read context from file '{context}': {e}"
                    )
                context_config = KiaraContextConfig(**data)
        elif isinstance(context, str):
            context_config = self.get_context_config(context_name=context)
        elif isinstance(context, uuid.UUID):
            context_config = self.find_context_config(context_id=context)
        else:
            raise Exception(
                f"Can't retrieve context, invalid context config type '{type(context)}'."
            )

        assert context_config.context_id not in self._contexts.keys()

        if extra_pipelines:
            if isinstance(extra_pipelines, str):
                extra_pipelines = [extra_pipelines]
            context_config.add_pipelines(*extra_pipelines)

        from kiara.context import Kiara

        kiara = Kiara(config=context_config, runtime_config=self.runtime_config)
        assert kiara.id == uuid.UUID(context_config.context_id)
        self._contexts[kiara.id] = kiara

        return kiara

    def find_context_config(self, context_id: uuid.UUID) -> KiaraContextConfig:
        raise NotImplementedError()

    def save(self, path: Union[Path, None] = None):
        if path is None:
            path = Path(KIARA_MAIN_CONFIG_FILE)

        if path.exists():
            raise Exception(
                f"Can't save config file, path already exists: {path.as_posix()}"
            )

        path.parent.mkdir(parents=True, exist_ok=True)

        data = self.model_dump(
            exclude={
                "context",
                "auto_generate_contexts",
                "stores_base_path",
                "context_search_paths",
                "default_context",
                "runtime_config",
            }
        )

        with path.open("wt") as f:
            yaml.dump(
                data,
                f,
            )

        self._config_path = path

    def delete(
        self, context_name: Union[str, None] = None, dry_run: bool = True
    ) -> Union["ContextInfo", None]:
        """Deletes the context with the specified name."""

        if context_name is None:
            context_name = self.default_context

        from kiara.context import Kiara
        from kiara.models.context import ContextInfo

        context_config = self.get_context_config(
            context_name=context_name, auto_generate=False
        )

        context_summary = None

        try:
            kiara = Kiara(config=context_config, runtime_config=self.runtime_config)

            context_summary = ContextInfo.create_from_context(
                kiara=kiara, context_name=context_name
            )

            if dry_run:
                return context_summary

            for archive in kiara.get_all_archives().keys():
                archive.delete_archive(archive_id=archive.archive_id)
        except Exception as e:
            log_message("delete.context.error", context_name=context_name, error=e)

        if not dry_run:
            if context_config._context_config_path is not None:
                os.unlink(context_config._context_config_path)

        return context_summary

    def create_renderable(self, **render_config: Any):
        from kiara.utils.output import create_recursive_table_from_model_object

        return create_recursive_table_from_model_object(
            self, render_config=render_config
        )


# class KiaraCurrentContextConfig(KiaraBaseConfig):
#     """Configuration that holds the currently active context, as well as references to other available contexts."""
#
#     class Config:
#         env_prefix = "kiara_context_"
#         extra = Extra.forbid
#
#         @classmethod
#         def customise_sources(
#             cls,
#             init_settings,
#             env_settings,
#             file_secret_settings,
#         ):
#             return (
#                 init_settings,
#                 env_settings,
#             )
#
#     kiara_config: KiaraConfig = Field(
#         description="The base kiara configuration.", default_factory=KiaraConfig
#     )
#     context: str = Field(
#         description=f"The path to an existing folder that houses the context, or the name of the context to use under the default kiara app data directory ({kiara_app_dirs.user_data_dir})."
#     )
#     context_configs: Dict[str, KiaraContextConfig] = Field(
#         description="The context configuration."
#     )
#     # overlay_config: KiaraConfig = Field(description="Extra config options to add to the selected context.")
#
#     @classmethod
#     def find_current_contexts(
#         cls, kiara_config: KiaraConfig
#     ) -> Dict[str, KiaraContextConfig]:
#
#         contexts: Dict[str, KiaraContextConfig] = {}
#
#         if not os.path.exists(kiara_config.context_base_path):
#             return contexts
#
#         for f in os.listdir(kiara_config.context_base_path):
#
#             config_dir = os.path.join(kiara_config.context_base_path, f)
#             k_config = cls.load_context(config_dir)
#             if k_config:
#                 contexts[k_config.context_alias] = k_config
#
#         return contexts
#
#     @classmethod
#     def create_context(
#         cls,
#         path: str,
#         context_id: str,
#         kiara_config: KiaraConfig,
#         context_alias: Optional[str],
#     ) -> KiaraContextConfig:
#
#         if os.path.exists(path):
#             raise Exception(f"Can't create kiara context folder, path exists: {path}")
#
#         os.makedirs(path, exist_ok=False)
#
#         config = {}
#         config["context_id"] = context_id
#         if not context_alias:
#             context_alias = config["context_id"]
#         config["context_alias"] = context_alias
#         config["context_folder"] = path
#
#         config["archives"] = create_default_archives(kiara_config=kiara_config)
#
#         kiara_context_config = KiaraContextConfig(**config)
#         config_file = os.path.join(path, "kiara_context.yaml")
#
#         with open(config_file, "wt") as f:
#             yaml.dump(kiara_config.dict(), f)
#
#         return kiara_context_config
#
#     @classmethod
#     def load_context(cls, path: str):
#
#         if path.endswith("kiara_context.yaml"):
#             path = os.path.dirname(path)
#
#         if not os.path.isdir(path):
#             return None
#
#         config_file = os.path.join(path, "kiara_context.yaml")
#         if not os.path.isfile(config_file):
#             return None
#
#         try:
#             config = get_data_from_file(config_file)
#             k_config = KiaraContextConfig(**config)
#         except Exception as e:
#             log_message("config.parse.error", config_file=config_file, error=e)
#             return None
#
#         return k_config
#
#     @root_validator(pre=True)
#     def validate_global_config(cls, values):
#
#         create_context = values.pop("create_context", False)
#
#         kiara_config = values.get("kiara_config", None)
#         if kiara_config is None:
#             kiara_config = KiaraConfig()
#             values["kiara_config"] = kiara_config
#
#         contexts = cls.find_current_contexts(kiara_config=kiara_config)
#
#         assert "context_configs" not in values.keys()
#         assert "overlay_config" not in values.keys()
#
#         context_name: Optional[str] = values.get("context", None)
#         if context_name is None:
#             context_name = kiara_config.default_context
#         loaded_context: Optional[KiaraContextConfig] = None
#
#         assert context_name != "kiara_context.yaml"
#
#         if context_name != DEFAULT_CONTEXT_NAME:
#             context_dir: Optional[str] = None
#             if context_name.endswith("kiara_context.yaml"):
#                 context_dir = os.path.dirname(context_name)
#             elif os.path.isdir(context_name):
#                 context_config = os.path.join(context_name, "kiara_context.yaml")
#                 if os.path.exists(context_config):
#                     context_dir = context_name
#
#             if context_dir is not None:
#                 loaded_context = loaded_context(context_dir)
#             elif create_context and os.path.sep in context_name:
#                 # we assume this is meant to be a path that is outside of the 'normal' kiara data directory
#                 if context_name.endswith("kiara_context.yaml"):
#                     context_dir = os.path.dirname(context_name)
#                 else:
#                     context_dir = os.path.abspath(os.path.expanduser(context_name))
#                 context_id = str(uuid.uuid4())
#                 loaded_context = cls.create_context(
#                     path=context_dir, context_id=context_id, kiara_config=kiara_config
#                 )
#
#         if loaded_context is not None:
#             contexts[loaded_context.context_alias] = loaded_context
#             context_name = loaded_context.context_alias
#         else:
#             match = None
#
#             for context_alias, context in contexts.items():
#
#                 if context.context_id == context_name:
#                     if match:
#                         raise Exception(
#                             f"More then one kiara contexts with id: {context.context_id}"
#                         )
#                     match = context_name
#                 elif context.context_alias == context_name:
#                     if match:
#                         raise Exception(
#                             f"More then one kiara contexts with alias: {context.context_id}"
#                         )
#                     match = context_name
#
#             if not match:
#                 if not create_context and context_name != DEFAULT_CONTEXT_NAME:
#                     raise Exception(f"Can't find context with name: {context_name}")
#
#                 context_id = str(uuid.uuid4())
#                 context_dir = os.path.join(kiara_config.context_base_path, context_id)
#
#                 kiara_config = cls.create_context(
#                     path=context_dir,
#                     context_id=context_id,
#                     context_alias=context_name,
#                     kiara_config=kiara_config,
#                 )
#                 contexts[context_name] = kiara_config
#             else:
#                 context_name = match
#
#         values["context"] = context_name
#         values["context_configs"] = contexts
#         values["archives"] = contexts[context_name].archives
#
#         return values
#
#     def get_context(self, context_name: Optional[str] = None) -> KiaraContextConfig:
#
#         if not context_name:
#             context_name = self.context
#
#         if context_name not in self.context_configs.keys():
#             raise Exception(
#                 f"Kiara context '{context_name}' not registered. Registered contexts: {', '.join(self.context_configs.keys())}"
#             )
#
#         selected_dict = self.context_configs[context_name].dict()
#         overlay = self.dict(exclude={"context", "context_configs", "kiara_config"})
#         selected_dict.update(overlay)
#
#         kc = KiaraContextConfig(**selected_dict)
#         return kc
#
#     def create_renderable(self, **config) -> RenderableType:
#         return create_table_from_model_object(self)
