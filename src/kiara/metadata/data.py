# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara.module_config import ModuleConfig


class DeserializeConfig(ModuleConfig):

    # value_id: str = Field(description="The id of the value.")
    serialization_type: str = Field(description="The serialization type.")
    input: typing.Any = Field(
        description="The inputs to use when running this module.", default_factory=dict
    )
    output_name: str = Field(description="The name of the output field for the value.")


class LoadConfig(ModuleConfig):

    value_id: str = Field(description="The id of the value.")
    base_path_input_name: typing.Optional[str] = Field(
        description="The name of the input that stores the base_path where the value is saved.",
        default=None,
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
