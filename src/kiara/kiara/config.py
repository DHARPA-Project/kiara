# -*- coding: utf-8 -*-
import os
import uuid
from pydantic import root_validator
from pydantic.config import Extra
from pydantic.env_settings import BaseSettings
from pydantic.fields import Field
from ruamel import yaml
from typing import Any, Dict, List, Optional

from kiara.defaults import KIARA_MAIN_CONFIG_FILE, kiara_app_dirs
from kiara.utils import get_data_from_file, log_message
from kiara.utils.db import get_kiara_db_url

yaml = yaml.YAML()


def config_file_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    if os.path.isfile(KIARA_MAIN_CONFIG_FILE):
        config = get_data_from_file(KIARA_MAIN_CONFIG_FILE)
    else:
        config = {}
    return config


class KiaraBaseConfig(BaseSettings):
    class Config:
        extra = Extra.forbid

    module_managers: List[str] = Field(
        description="The module managers to use in this kiara instance.",
        default_factory=list,
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

        contexts = {}

        if not os.path.exists(kiara_app_dirs.user_data_dir):
            return contexts

        for f in os.listdir(kiara_app_dirs.user_data_dir):

            config_dir = os.path.join(kiara_app_dirs.user_data_dir, f)
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
                context_dir = os.path.join(kiara_app_dirs.user_data_dir, context_id)

                kiara_config = cls.create_context(
                    path=context_dir, context_id=context_id, context_alias=context_name
                )
                contexts[context_name] = kiara_config
            else:
                context_name = match

        values["context"] = context_name
        values["context_configs"] = contexts

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
