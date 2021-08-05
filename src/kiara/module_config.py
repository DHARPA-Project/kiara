# -*- coding: utf-8 -*-

"""Configuration models for the *Kiara* package."""
import deepdiff
import json
import os
import typing
from pathlib import Path
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
    from kiara.pipeline.config import PipelineModuleConfig


class ModuleTypeConfig(BaseModel):
    """Base class that describes the configuration a [``KiaraModule``][kiara.module.KiaraModule] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``KiaraModule`` class. By default,
    such a ``KiaraModule`` is not configurable.

    There are two config options every ``KiaraModule`` supports:

     - ``constants``, and
     - ``defaults``

     Constants are pre-set inputs, and users can't change them and an error is thrown if they try. Defaults are default
     values that override the schema defaults, and those can be overwritten by users. If both a constant and a default
     value is set for an input field, an error is thrown.
    """

    @classmethod
    def requires_config(cls) -> bool:

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


class ModuleInstanceConfig(KiaraInfoModel):
    @classmethod
    def from_file(cls, path: typing.Union[str, Path]):

        data = get_data_from_file(path)
        return ModuleInstanceConfig(module_type="pipeline", module_config=data)

    @classmethod
    def create(
        cls,
        config: typing.Union["ModuleInstanceConfig", typing.Mapping, str],
        module_config: typing.Union[
            None, typing.Mapping[str, typing.Any], "PipelineModuleConfig"
        ] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if kiara is None:
            kiara = Kiara.instance()

        if isinstance(config, typing.Mapping):

            if module_config:
                raise NotImplementedError()

            operation_config: ModuleInstanceConfig = ModuleInstanceConfig(**config)

        elif isinstance(config, str):
            if config == "pipeline":
                if not module_config:
                    raise Exception(
                        "Can't create workflow from 'pipeline' module type without further configuration."
                    )
                elif isinstance(module_config, typing.Mapping):
                    operation_config = ModuleInstanceConfig(
                        module_type="pipeline", module_config=module_config
                    )
                else:
                    from kiara.pipeline.config import PipelineModuleConfig

                    if isinstance(config, PipelineModuleConfig):
                        operation_config = ModuleInstanceConfig(
                            module_type="pipeline", module_config=config.dict()
                        )
                    else:
                        raise Exception(
                            f"Can't create operation config, invalid type for 'module_config': {type(module_config)}"
                        )

            elif config in kiara.available_module_types:
                if module_config:
                    operation_config = ModuleInstanceConfig(
                        module_type=config, module_config=module_config
                    )
                else:
                    operation_config = ModuleInstanceConfig(module_type=config)
            elif config in kiara.operation_mgmt.profiles.keys():
                operation_config = kiara.operation_mgmt.profiles[config]
            elif os.path.isfile(os.path.expanduser(config)):
                path = os.path.expanduser(config)
                workflow_config_data = get_data_from_file(path)

                if module_config:
                    raise NotImplementedError()

                operation_config = ModuleInstanceConfig(
                    module_type="pipeline", module_config=workflow_config_data
                )
            else:
                raise Exception(
                    f"Can't create workflow config from string '{config}'. Value must be path to a file, or one of: {', '.join(kiara.available_module_types)}"
                )
        elif isinstance(config, ModuleInstanceConfig):
            operation_config = config
            if module_config:
                raise NotImplementedError()
        else:
            raise TypeError(
                f"Invalid type '{type(config)}' for workflow configuration."
            )

        return operation_config

    @classmethod
    def create_renderable_from_module_instance_configs(
        cls,
        configs: typing.Mapping[str, "ModuleInstanceConfig"],
        **render_config: typing.Any,
    ):

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
    doc: typing.Optional[DocumentationMetadataModel] = Field(
        description="Documentation for this operation.", default=None
    )

    @validator("doc", pre=True)
    def create_doc(cls, value):
        if value is None:
            return None
        return DocumentationMetadataModel.create(value)

    def create_module(self, kiara: "Kiara"):

        if self._module is None:
            self._module = kiara.create_module(
                id=f"__{self.module_type}",
                module_type=self.module_type,
                module_config=self.module_config,
            )
        return self._module

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        conf = Syntax(
            self.json(exclude_none=True, indent=2),
            "json",
            background_color="default",
        )
        return conf

    def create_config_renderable(self, **config: typing.Any) -> RenderableType:

        c = {"module_type": self.module_type, "module_config": self.module_config}
        conf_json = json.dumps(c, indent=2)
        conf = Syntax(
            conf_json,
            "json",
            background_color="default",
        )
        return conf


KIARA_CONFIG = typing.TypeVar("KIARA_CONFIG", bound=ModuleTypeConfig)
