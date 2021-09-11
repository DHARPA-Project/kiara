# -*- coding: utf-8 -*-

"""Module-related configuration models for the *Kiara* package."""

import deepdiff
import os
import typing
from pydantic import BaseModel, Extra, Field, PrivateAttr, validator
from rich import box
from rich.console import Console, ConsoleOptions, RenderableType, RenderResult
from rich.syntax import Syntax
from rich.table import Table

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.info import KiaraInfoModel
from kiara.metadata.core_models import DocumentationMetadataModel
from kiara.utils import get_data_from_file

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.module import KiaraModule
    from kiara.pipeline.config import PipelineConfig


class ModuleTypeConfigSchema(BaseModel):
    """Base class that describes the configuration a [``KiaraModule``][kiara.module.KiaraModule] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``KiaraModule`` class.

    There are two config options every ``KiaraModule`` supports:

     - ``constants``, and
     - ``defaults``

     Constants are pre-set inputs, and users can't change them and an error is thrown if they try. Defaults are default
     values that override the schema defaults, and those can be overwritten by users. If both a constant and a default
     value is set for an input field, an error is thrown.
    """

    @classmethod
    def requires_config(cls) -> bool:
        """Return whether this class can be used as-is, or requires configuration before an instance can be created."""

        for field_name, field in cls.__fields__.items():
            if field.required and field.default is None:
                return True
        return False

    _config_hash: str = PrivateAttr(default=None)
    constants: typing.Dict[str, typing.Any] = Field(
        default_factory=dict, description="Value constants for this module."
    )
    defaults: typing.Dict[str, typing.Any] = Field(
        default_factory=dict, description="Value defaults for this module."
    )

    class Config:
        extra = Extra.forbid
        validate_assignment = True

    def get(self, key: str) -> typing.Any:
        """Get the value for the specified configuation key."""

        if key not in self.__fields__:
            raise Exception(
                f"No config value '{key}' in module config class '{self.__class__.__name__}'."
            )

        return getattr(self, key)

    @property
    def config_hash(self):

        if self._config_hash is None:
            _d = self.dict()
            hashes = deepdiff.DeepHash(_d)
            self._config_hash = hashes[_d]
        return self._config_hash

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False

        return self.dict() == other.dict()

    def __hash__(self):

        return hash(self.config_hash)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        my_table = Table(box=box.MINIMAL, show_header=False)
        my_table.add_column("Field name", style="i")
        my_table.add_column("Value")
        for field in self.__fields__:
            my_table.add_row(field, getattr(self, field))

        yield my_table


def parse_and_create_module_config(
    config: typing.Union[
        "ModuleConfig",
        typing.Mapping,
        str,
        typing.Type["KiaraModule"],
        "PipelineConfig",
    ],
    module_config: typing.Union[None, typing.Mapping[str, typing.Any]] = None,
    kiara: typing.Optional["Kiara"] = None,
) -> "ModuleConfig":
    """Utility method to create a `ModuleConfig` object from different input types.

    Arguments:
        config: the 'main' configuation
        module_config: if the 'main' configuration value is an (unconfigured) module type, this argument can contain the configuration for the module instance
        kiara: the *kiara* context
    """

    if kiara is None:
        from kiara.kiara import Kiara

        kiara = Kiara.instance()

    if isinstance(config, type):
        from kiara.module import KiaraModule

        if issubclass(config, KiaraModule):
            # this basically makes sure the class is augmented with the '_module_type_id` attribute
            if not hasattr(config, "_module_type_id"):
                match = False
                for mod_name in kiara.available_module_types:
                    mod_cls = kiara.get_module_class(mod_name)
                    if mod_cls == config:
                        match = True
                        break
                if not match:
                    raise Exception(f"Class '{config}' not a valid kiara module.")

            config = config._module_type_id  # type: ignore
        else:
            raise TypeError(f"Invalid type '{type(config)}' for module configuration.")

    if isinstance(config, typing.Mapping):

        if module_config:
            raise NotImplementedError()

        module_instance_config: ModuleConfig = ModuleConfig(**config)

    elif isinstance(config, str):
        if config == "pipeline":
            if not module_config:
                raise Exception(
                    "Can't create module from 'pipeline' module type without further configuration."
                )
            elif isinstance(module_config, typing.Mapping):
                module_instance_config = ModuleConfig(
                    module_type="pipeline", module_config=module_config
                )
            else:
                from kiara.pipeline.config import PipelineConfig

                if isinstance(config, PipelineConfig):
                    module_instance_config = ModuleConfig(
                        module_type="pipeline", module_config=config.dict()
                    )
                else:
                    raise Exception(
                        f"Can't create module config, invalid type for 'module_config' parameter: {type(module_config)}"
                    )

        elif config in kiara.available_module_types:
            if module_config:
                module_instance_config = ModuleConfig(
                    module_type=config, module_config=module_config
                )
            else:
                module_instance_config = ModuleConfig(module_type=config)
        elif config in kiara.operation_mgmt.profiles.keys():
            module_instance_config = kiara.operation_mgmt.profiles[config]
        elif os.path.isfile(os.path.expanduser(config)):
            path = os.path.expanduser(config)
            module_config_data = get_data_from_file(path)

            if module_config:
                raise NotImplementedError()

            if not isinstance(module_config_data, typing.Mapping):
                raise Exception(
                    f"Invalid module/pipeline config, must be a mapping type: {module_config_data}"
                )

            if "steps" in module_config_data.keys():
                module_type = "pipeline"
                module_instance_config = ModuleConfig(
                    module_type=module_type, module_config=module_config_data
                )
            elif "module_type" in module_config_data.keys():
                module_instance_config = ModuleConfig(**module_config_data)
        else:
            raise Exception(
                f"Can't create module config from string '{config}'. Value must be path to a file, or one of: {', '.join(kiara.available_module_types)}"
            )
    elif isinstance(config, ModuleConfig):
        module_instance_config = config
        if module_config:
            raise NotImplementedError()
    else:

        from kiara.pipeline.config import PipelineConfig

        if isinstance(config, PipelineConfig):
            module_instance_config = ModuleConfig(
                module_type="pipeline", module_config=config.dict()
            )
        else:
            raise TypeError(f"Invalid type '{type(config)}' for module configuration.")

    return module_instance_config


class ModuleConfig(KiaraInfoModel):
    """A class to hold the type and configuration for a module instance."""

    @classmethod
    def create_module_config(
        cls,
        config: typing.Union[
            "ModuleConfig",
            typing.Mapping,
            str,
            typing.Type["KiaraModule"],
            "PipelineConfig",
        ],
        module_config: typing.Union[None, typing.Mapping[str, typing.Any]] = None,
        kiara: typing.Optional["Kiara"] = None,
    ) -> "ModuleConfig":

        conf = parse_and_create_module_config(
            config=config, module_config=module_config, kiara=kiara
        )
        return conf

    @classmethod
    def create_renderable_from_module_instance_configs(
        cls,
        configs: typing.Mapping[str, "ModuleConfig"],
        **render_config: typing.Any,
    ):
        """Convenience method to create a renderable for this module configuration, to be printed to terminal."""

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Id", style="i", no_wrap=True)
        table.add_column("Description")
        if not render_config.get("omit_config", False):
            table.add_column("Configuration")

        for id, config in configs.items():
            if config.doc:
                doc = config.doc.description
            else:
                doc = DEFAULT_NO_DESC_VALUE

            row: typing.List[RenderableType] = [id, doc]
            if not render_config.get("omit_config", False):
                conf = config.json(exclude={"doc"}, indent=2)
                row.append(Syntax(conf, "json", background_color="default"))
            table.add_row(*row)

        return table

    class Config:
        extra = Extra.forbid
        validate_all = True

    _module: typing.Optional["KiaraModule"] = PrivateAttr(default=None)
    module_type: str = Field(description="The module type.")
    module_config: typing.Dict[str, typing.Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )
    doc: DocumentationMetadataModel = Field(
        description="Documentation for this operation.", default=None
    )

    @validator("doc", pre=True)
    def create_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    def create_module(
        self,
        kiara: typing.Optional["Kiara"] = None,
        module_id: typing.Optional[str] = None,
    ) -> "KiaraModule":
        """Create a module instance from this configuration."""

        if module_id and not isinstance(module_id, str):
            raise TypeError(
                f"Invalid type, module_id must be a string, not: {type(module_id)}"
            )

        if kiara is None:
            from kiara.kiara import Kiara

            kiara = Kiara.instance()

        if self._module is None:
            self._module = kiara.create_module(
                id=module_id,
                module_type=self.module_type,
                module_config=self.module_config,
            )
        return self._module

    def create_renderable(self, **config: typing.Any) -> RenderableType:
        """Create a renderable for this module configuration."""

        conf = Syntax(
            self.json(exclude_none=True, indent=2),
            "json",
            background_color="default",
        )
        return conf


KIARA_CONFIG = typing.TypeVar("KIARA_CONFIG", bound=ModuleTypeConfigSchema)
