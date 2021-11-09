# -*- coding: utf-8 -*-
import os
import typing
from pydantic import BaseSettings, Extra, Field, validator

from kiara.defaults import KIARA_DATA_STORE_DIR, kiara_app_dirs
from kiara.module_mgmt import ModuleManager
from kiara.module_mgmt.pipelines import PipelineModuleManagerConfig
from kiara.module_mgmt.python_classes import PythonModuleManagerConfig
from kiara.processing.parallel import ThreadPoolProcessorConfig
from kiara.processing.synchronous import SynchronousProcessorConfig
from kiara.utils import get_data_from_file


def yaml_config_settings_source(settings: BaseSettings) -> typing.Dict[str, typing.Any]:
    """
    A simple settings source that loads variables from a JSON file
    at the project's root.

    Here we happen to choose to use the `env_file_encoding` from Config
    when reading `config.json`
    """

    config_file = os.path.join(kiara_app_dirs.user_config_dir, "config.yaml")
    if os.path.exists(config_file):
        data = get_data_from_file(config_file)
        return data
    else:
        return {}


class KiaraConfig(BaseSettings):
    class Config:
        extra = Extra.forbid
        env_file_encoding = "utf-8"
        env_prefix = "kiara_"

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
                yaml_config_settings_source,
                file_secret_settings,
            )

    module_managers: typing.Optional[
        typing.List[
            typing.Union[PythonModuleManagerConfig, PipelineModuleManagerConfig]
        ]
    ] = Field(
        description="The module managers to use in this kiara instance.", default=None
    )
    default_processor: typing.Optional[
        typing.Union[SynchronousProcessorConfig, ThreadPoolProcessorConfig]
    ] = Field(
        description="The configuration for the default processor to use.",
        default_factory=SynchronousProcessorConfig,
    )
    data_store: str = Field(
        description="The path to the local kiara data store.",
        default=KIARA_DATA_STORE_DIR,
    )
    extra_pipeline_folders: typing.List[str] = Field(
        description="Paths to local folders that contain kiara pipelines.",
        default_factory=list,
    )
    ignore_errors: bool = Field(
        description="If set, kiara will try to ignore most errors (that can be ignored).",
        default=False,
    )

    @validator("module_managers", pre=True)
    def _validate_managers(cls, v):

        if v is None:
            return []

        if isinstance(v, typing.Mapping):
            v = [v]

        assert isinstance(v, typing.Iterable)

        result = []
        for item in v:
            if isinstance(item, ModuleManager):
                result.append(item)
            else:
                assert isinstance(item, typing.Mapping)
                mm_type = item.get("module_manager_type", None)
                if not mm_type:
                    raise ValueError(
                        f"No module manager type provided in config: {item}"
                    )

                if mm_type == "python":
                    item_config = PythonModuleManagerConfig(**item)
                elif mm_type == "pipeline":
                    item_config = PipelineModuleManagerConfig(**item)
                else:
                    raise ValueError(f"Invalid module manager type: {mm_type}")
                result.append(item_config)

        return result

    @validator("default_processor", pre=True)
    def _validate_default_processor(cls, v):

        if not v:
            return SynchronousProcessorConfig()

        if isinstance(v, (SynchronousProcessorConfig, ThreadPoolProcessorConfig)):
            return v

        if v == "synchronous":
            return SynchronousProcessorConfig()

        if v == "multi-threaded":
            return ThreadPoolProcessorConfig()

        if not isinstance(v, typing.Mapping):
            raise ValueError(
                f"Invalid type '{type(v)}' for default_processor config: {v}"
            )
        processor_type = v.get("module_processor_type", None)
        if not processor_type:
            raise ValueError("No 'module_processor_type' provided: {config}")
        if processor_type == "synchronous":
            config = SynchronousProcessorConfig(**v)
        elif processor_type == "multi-threaded":
            config = ThreadPoolProcessorConfig()

        return config
