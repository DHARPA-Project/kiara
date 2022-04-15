# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


import os
import uuid
from pydantic import BaseModel, root_validator
from pydantic.config import Extra
from pydantic.env_settings import BaseSettings
from pydantic.fields import Field
from ruamel import yaml as r_yaml
from typing import Any, Dict, List, Mapping, Optional

from kiara.defaults import (
    DEFAULT_ALIAS_STORE_MARKER,
    DEFAULT_DATA_STORE_MARKER,
    DEFAULT_JOB_STORE_MARKER,
    KIARA_CONTEXTS_FOLDER,
    KIARA_MAIN_CONFIG_FILE,
    KIARA_STORES_FOLDER,
    kiara_app_dirs,
)
from kiara.registries.environment import EnvironmentRegistry
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import get_data_from_file, log_message
from kiara.utils.db import get_kiara_db_url

yaml = r_yaml.YAML()


def config_file_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    if os.path.isfile(KIARA_MAIN_CONFIG_FILE):
        config = get_data_from_file(KIARA_MAIN_CONFIG_FILE)
    else:
        config = {}
    return config


class KiaraArchiveConfig(BaseModel):

    archive_id: str = Field(description="The unique archive id.")
    archive_type: str = Field(description="The archive type.")
    config: Mapping[str, Any] = Field(
        description="Archive type specific config.", default_factory=dict
    )

    @property
    def archive_uuid(self) -> uuid.UUID:
        return uuid.UUID(self.archive_id)


def create_default_archives():

    env_registry = EnvironmentRegistry.instance()

    archives = env_registry.environments["kiara_types"].archive_types
    data_store_type = "filesystem_data_store"

    assert data_store_type in archives.keys()

    data_store_id = ID_REGISTRY.generate(comment="default data store id")
    data_archive_config = {
        "base_path": os.path.join(KIARA_STORES_FOLDER, data_store_type)
    }
    data_store = KiaraArchiveConfig.construct(
        archive_id=str(data_store_id),
        archive_type=data_store_type,
        config=data_archive_config,
    )

    job_store_type = "filesystem_job_store"
    job_archive_config = {
        "base_path": os.path.join(KIARA_STORES_FOLDER, job_store_type)
    }
    job_store_id = ID_REGISTRY.generate(comment="default job store id")
    job_store = KiaraArchiveConfig.construct(
        archive_id=str(job_store_id),
        archive_type=job_store_type,
        config=job_archive_config,
    )

    alias_store_type = "filesystem_alias_store"
    alias_store_config = {
        "base_path": os.path.join(KIARA_STORES_FOLDER, alias_store_type)
    }
    alias_store_id = ID_REGISTRY.generate(comment="default alias store id")
    alias_store = KiaraArchiveConfig.construct(
        archive_id=str(alias_store_id),
        archive_type=alias_store_type,
        config=alias_store_config,
    )

    return {
        DEFAULT_DATA_STORE_MARKER: data_store,
        DEFAULT_JOB_STORE_MARKER: job_store,
        DEFAULT_ALIAS_STORE_MARKER: alias_store,
    }


class KiaraBaseConfig(BaseSettings):
    class Config:
        extra = Extra.forbid

    archives: Dict[str, KiaraArchiveConfig] = Field(
        description="All the archives this kiara context can use and the aliases they are registered with."
    )
    extra_pipeline_folders: List[str] = Field(
        description="Paths to local folders that contain kiara pipelines.",
        default_factory=list,
    )
    ignore_errors: bool = Field(
        description="If set, kiara will try to ignore most errors (that can be ignored).",
        default=False,
    )


class KiaraContextConfig(KiaraBaseConfig):
    class Config:
        extra = Extra.forbid

    context_id: str = Field(description="A globally unique id for this kiara context.")
    context_alias: str = Field(
        description="An alias for this kiara context, must be unique within all known contexts."
    )
    context_folder: str = Field(
        description="The base folder where settings and data for this kiara context will be stored."
    )

    @property
    def db_url(self):
        return get_kiara_db_url(self.context_folder)

    @property
    def data_directory(self) -> str:
        return os.path.join(self.context_folder, "data")


class KiaraGlobalConfig(KiaraBaseConfig):
    class Config:
        env_prefix = "kiara_"
        extra = Extra.forbid

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                config_file_settings_source,
            )

    context: str = Field(
        description=f"The path to an existing folder that houses the context, or the name of the context to use under the default kiara app data directory ({kiara_app_dirs.user_data_dir}).",
        default="default_context",
    )
    context_configs: Dict[str, KiaraContextConfig] = Field(
        description="The context configuration."
    )
    # overlay_config: KiaraConfig = Field(description="Extra config options to add to the selected context.")

    @classmethod
    def find_current_contexts(cls) -> Dict[str, KiaraContextConfig]:

        contexts: Dict[str, KiaraContextConfig] = {}

        if not os.path.exists(KIARA_CONTEXTS_FOLDER):
            return contexts

        for f in os.listdir(KIARA_CONTEXTS_FOLDER):

            config_dir = os.path.join(KIARA_CONTEXTS_FOLDER, f)
            k_config = cls.load_context(config_dir)
            if k_config:
                contexts[k_config.context_alias] = k_config

        return contexts

    @classmethod
    def create_context(
        cls, path: str, context_id: str, context_alias: Optional[str]
    ) -> KiaraContextConfig:

        if os.path.exists(path):
            raise Exception(f"Can't create kiara context folder, path exists: {path}")

        os.makedirs(path, exist_ok=False)

        config = {}
        config["context_id"] = context_id
        if not context_alias:
            context_alias = config["context_id"]
        config["context_alias"] = context_alias
        config["context_folder"] = path

        config["archives"] = create_default_archives()

        kiara_config = KiaraContextConfig(**config)
        config_file = os.path.join(path, "kiara_context.yaml")

        with open(config_file, "wt") as f:
            yaml.dump(kiara_config.dict(), f)

        return kiara_config

    @classmethod
    def load_context(cls, path: str):

        if path.endswith("kiara_context.yaml"):
            path = os.path.dirname(path)

        if not os.path.isdir(path):
            return None

        config_file = os.path.join(path, "kiara_context.yaml")
        if not os.path.isfile(config_file):
            return None

        try:
            config = get_data_from_file(config_file)
            k_config = KiaraContextConfig(**config)
        except Exception as e:
            log_message("config.parse.error", config_file=config_file, error=e)
            return None

        return k_config

    @root_validator(pre=True)
    def validate_global_config(cls, values):

        create_context = values.pop("create_context", False)
        contexts = cls.find_current_contexts()

        assert "context_configs" not in values.keys()
        assert "overlay_config" not in values.keys()

        context_name: str = values.get("context", "default_context")
        loaded_context: Optional[KiaraContextConfig] = None

        assert context_name != "kiara_context.yaml"

        if context_name != "default_context":
            context_dir: Optional[str] = None
            if context_name.endswith("kiara_context.yaml"):
                context_dir = os.path.dirname(context_name)
            elif os.path.isdir(context_name):
                context_config = os.path.join(context_name, "kiara_context.yaml")
                if os.path.exists(context_config):
                    context_dir = context_name

            if context_dir is not None:
                loaded_context = loaded_context(context_dir)
            elif create_context and os.path.sep in context_name:
                # we assume this is meant to be a path that is outside of the 'normal' kiara data directory
                if context_name.endswith("kiara_context.yaml"):
                    context_dir = os.path.dirname(context_name)
                else:
                    context_dir = os.path.abspath(os.path.expanduser(context_name))
                context_id = str(uuid.uuid4())
                loaded_context = cls.create_context(
                    path=context_dir, context_id=context_id
                )

        if loaded_context is not None:
            contexts[loaded_context.context_alias] = loaded_context
            context_name = loaded_context.context_alias
        else:
            match = None

            for context_alias, context in contexts.items():

                if context.context_id == context_name:
                    if match:
                        raise Exception(
                            f"More then one kiara contexts with id: {context.context_id}"
                        )
                    match = context_name
                elif context.context_alias == context_name:
                    if match:
                        raise Exception(
                            f"More then one kiara contexts with alias: {context.context_id}"
                        )
                    match = context_name

            if not match:
                if not create_context and context_name != "default_context":
                    raise Exception(f"Can't find context with name: {context_name}")

                context_id = str(uuid.uuid4())
                context_dir = os.path.join(KIARA_CONTEXTS_FOLDER, context_id)

                kiara_config = cls.create_context(
                    path=context_dir, context_id=context_id, context_alias=context_name
                )
                contexts[context_name] = kiara_config
            else:
                context_name = match

        values["context"] = context_name
        values["context_configs"] = contexts
        values["archives"] = contexts[context_name].archives

        return values

    def get_context(self, context_name: Optional[str] = None) -> KiaraContextConfig:

        if not context_name:
            context_name = self.context

        if context_name not in self.context_configs.keys():
            raise Exception(
                f"Kiara context '{context_name}' not registered. Registered contexts: {', '.join(self.context_configs.keys())}"
            )

        selected_dict = self.context_configs[context_name].dict()
        overlay = self.dict(exclude={"context", "context_configs"})
        selected_dict.update(overlay)

        kc = KiaraContextConfig(**selected_dict)
        return kc
