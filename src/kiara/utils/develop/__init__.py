# -*- coding: utf-8 -*-
from enum import Enum
from typing import Any, ClassVar, Dict, Tuple, Type, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
from rich.console import Group, RenderableType
from rich.panel import Panel

from kiara.utils import is_develop


def log_dev_message(msg: RenderableType, title: Union[str, None] = None):

    if not is_develop():
        return

    if not title:
        title = "Develop-mode message"
    panel = Panel(Group("", msg), title=f"[yellow]{title}[/yellow]", title_align="left")

    from kiara.utils.cli import terminal_print

    terminal_print(panel)


# def dev_config_file_settings_source(settings: BaseSettings) -> Dict[str, Any]:
#     """
#     A simple settings source that loads variables from a JSON file
#     at the project's root.
#
#     Here we happen to choose to use the `env_file_encoding` from Config
#     when reading `config.json`
#     """
#     if os.path.exists(KIARA_DEV_CONFIG_FILE):
#         dev_config: Dict[str, Any] = get_data_from_file(KIARA_DEV_CONFIG_FILE)
#     else:
#         dev_config = {}
#     return dev_config


# def profile_settings_source(settings: BaseSettings) -> Dict[str, Any]:
#
#     profile_name = os.environ.get("DEVELOP", None)
#     if not profile_name:
#         profile_name = os.environ.get("develop", None)
#     if not profile_name:
#         profile_name = os.environ.get("DEV", None)
#     if not profile_name:
#         profile_name = os.environ.get("dev", None)
#     if not profile_name:
#         profile_name = os.environ.get("DEV_PROFILE", None)
#     if not profile_name:
#         profile_name = os.environ.get("dev_profile", None)
#
#     result: Dict[str, Any] = {}
#     if not profile_name:
#         return result
#
#     profile_name = profile_name.lower()
#
#     for model in KiaraDevSettings.model_fields.values():
#         raise NotImplementedError("TODO: fix this after pydantic v2 refactoring")
#         if not issubclass(model.type_, BaseModel):
#             continue
#
#         profiles = getattr(model.type_, "PROFILES", None)
#         if not profiles:
#             continue
#
#         p = profiles.get(profile_name, None)
#         if not p:
#             continue
#         result[model.name] = p
#
#     return result


class DetailLevel(Enum):

    NONE = "none"
    MINIMAL = "minimal"
    FULL = "full"


class PreRunMsgDetails(BaseModel):
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, use_enum_values=True
    )

    pipeline_steps: bool = Field(
        description="Whether to also display information for modules that are run as part of a pipeline.",
        default=False,
    )
    module_info: DetailLevel = Field(
        description="Whether to display details about the module to be run.",
        default=DetailLevel.MINIMAL,
    )
    internal_modules: bool = Field(
        description="Whether to also print details about runs of internal modules.",
        default=False,
    )
    inputs_info: DetailLevel = Field(
        description="Whether to display details about the run inputs.",
        default=DetailLevel.MINIMAL,
    )


class PostRunMsgDetails(BaseModel):
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, use_enum_values=True
    )

    pipeline_steps: bool = Field(
        description="Whether to also display information for modules that are run as part of a pipeline",
        default=False,
    )
    module_info: DetailLevel = Field(
        description="Whether to display details about the module that was run.",
        default=DetailLevel.NONE,
    )
    internal_modules: bool = Field(
        description="Whether to also print details about runs of internal module.",
        default=False,
    )
    inputs_info: DetailLevel = Field(
        description="Whether to display details about the run inputs.",
        default=DetailLevel.NONE,
    )
    outputs_info: DetailLevel = Field(
        description="Whether to display details about the run outputs.",
        default=DetailLevel.MINIMAL,
    )


class KiaraDevLogSettings(BaseModel):

    PROFILES: ClassVar[Dict[str, Any]] = {
        "full": {
            "log_pre_run": True,
            "pre_run": {
                "pipeline_steps": True,
                "module_info": "full",
                "inputs_info": "full",
            },
            "log_post_run": True,
            "post_run": {
                "pipeline_steps": True,
                "module_info": "minimal",
                "inputs_info": "minimal",
                "outputs_info": "full",
            },
        },
        "internal": {
            "pre_run": {"internal_modules": True},
            "post_run": {"internal_modules": True},
        },
    }
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, use_enum_values=True
    )

    exc: DetailLevel = Field(
        description="How detailed to print exceptions", default=DetailLevel.MINIMAL
    )
    log_pre_run: bool = Field(
        description="Print details about a module and its inputs before running it.",
        default=True,
    )
    pre_run: PreRunMsgDetails = Field(
        description="Fine-grained settings about what to display in the pre-run message.",
        default_factory=PreRunMsgDetails,
    )
    log_post_run: bool = Field(
        description="Print details about the results of a module run.", default=True
    )
    post_run: PostRunMsgDetails = Field(
        description="Fine-grained settings aobut what to display in the post-run message.",
        default_factory=PostRunMsgDetails,
    )


class KiaraDevSettings(BaseSettings):
    # TODO[pydantic]: We couldn't refactor this class, please create the `model_config` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.

    model_config = SettingsConfigDict(
        extra="forbid",
        validate_assignment=True,
        use_enum_values=True,
        env_prefix="dev_",
        env_nested_delimiter="__",
    )

    # class Config:
    #
    #     extra = Extra.forbid
    #     validate_assignment = True
    #     env_prefix = "dev_"
    #     use_enum_values = True
    #     env_nested_delimiter = "__"
    #
    #     @classmethod
    #     def customise_sources(
    #         cls,
    #         init_settings,
    #         env_settings,
    #         file_secret_settings,
    #     ):
    #         return (
    #             init_settings,
    #             # profile_settings_source,
    #             dev_config_file_settings_source,
    #             env_settings,
    #         )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
        )

    log: KiaraDevLogSettings = Field(
        description="Settings about what messages to print in 'develop' mode, and what details to include.",
        default_factory=KiaraDevLogSettings,
    )
    job_cache: bool = Field(
        description="Whether to always disable the job cache (ignores the runtime_job_cache setting in the kiara configuration).",
        default=True,
    )

    def create_renderable(self, **render_config: Any):
        from kiara.utils.output import create_recursive_table_from_model_object

        return create_recursive_table_from_model_object(
            self, render_config=render_config
        )


KIARA_DEV_SETTINGS = KiaraDevSettings()
