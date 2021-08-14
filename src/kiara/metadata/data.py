# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara.module_config import ModuleConfig


class LoadConfig(ModuleConfig):

    value_id: str = Field(description="The id of the value.")
    base_path_input_name: str = Field(
        description="The base path where the value is stored.", default="base_path"
    )
    inputs: typing.Dict[str, typing.Any] = Field(
        description="The inputs to use when running this module.", default_factory=dict
    )
    output_name: str = Field(description="The name of the output field for the value.")


class SaveConfig(ModuleConfig):

    inputs: typing.Dict[str, typing.Any] = Field(
        description="The inputs to use when running this module.", default_factory=dict
    )
    load_config_output: str = Field(
        description="The output name that will contain the load config output value."
    )
