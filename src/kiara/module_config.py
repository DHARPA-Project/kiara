# -*- coding: utf-8 -*-

"""Configuration models for the *Kiara* package."""

import deepdiff
import typing
from pathlib import Path
from pydantic import BaseModel, Extra, Field, PrivateAttr
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.table import Table

from kiara.utils import get_data_from_file

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.module import KiaraModule


class OperationConfig(BaseModel):
    @classmethod
    def from_file(cls, path: typing.Union[str, Path]):

        data = get_data_from_file(path)
        return OperationConfig(module_type="pipeline", module_config=data)

    class Config:
        extra = Extra.forbid
        validate_all = True

    _module: typing.Optional["KiaraModule"] = PrivateAttr(default=None)
    module_type: str = Field(description="The module type.")
    module_config: typing.Dict[str, typing.Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )

    def create_module(self, kiara: "Kiara"):

        if self._module is None:
            self._module = kiara.create_module(
                id=f"extract_metadata_{self.module_type}",
                module_type=self.module_type,
                module_config=self.module_config,
            )
        return self._module


class KiaraModuleConfig(BaseModel):
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


KIARA_CONFIG = typing.TypeVar("KIARA_CONFIG", bound=KiaraModuleConfig)

# class OperationConfig(BaseModel):
#     """The object to hold a configuration for a workflow."""
#
#     class Config:
#         extra = Extra.forbid
#         validate_assignment = True
#
#     @classmethod
#     def from_file(cls, path: typing.Union[str, Path]):
#
#         data = get_data_from_file(path)
#         return OperationConfig(module_type="pipeline", module_config=data)
#
#     module_type: str = Field(
#         description="The name of the 'root' module of this workflow.",
#         default="pipeline",
#     )
#     module_config: typing.Dict[str, typing.Any] = Field(
#         default_factory=dict,
#         description="The configuration for the 'root' module of this workflow.",
#     )
