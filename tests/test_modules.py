# -*- coding: utf-8 -*-
import time
import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.values import ValueSchema
from kiara.module import StepInputs, StepOutputs


class DummyProcessingModuleConfig(KiaraModuleConfig):
    """Configuration for the 'dummy' processing module."""

    doc: typing.Optional[str] = None

    input_schema: typing.Mapping[str, typing.Mapping] = Field(
        description="The input schema for this module."
    )
    output_schema: typing.Mapping[str, typing.Mapping] = Field(
        description="The output schema for this module."
    )
    outputs: typing.Mapping[str, typing.Any] = Field(
        description="The (dummy) output for this module.", default_factory=dict
    )
    delay: float = Field(
        description="The delay in seconds from processing start to when the (dummy) outputs are returned.",
        default=0,
    )


class DummyModule(KiaraModule):
    """Module that simulates processing, but uses hard-coded outputs as a result."""

    _config_cls = DummyProcessingModuleConfig

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:
        """The input schema for the ``dummy`` module is created at object creation time from the ``input_schemas`` config parameter."""

        result = {}
        for k, v in self.config.get("input_schema").items():  # type: ignore
            schema = ValueSchema(**v)
            schema.validate_types(self._kiara)
            result[k] = schema
        return result

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        """The output schema for the ``dummy`` module is created at object creation time from the ``output_schemas`` config parameter."""

        result = {}
        for k, v in self.config.get("output_schema").items():  # type: ignore
            schema = ValueSchema(**v)
            schema.validate_types(self._kiara)
            result[k] = schema
        return result

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:
        """Returns the hardcoded output values that are set in the ``outputs`` config field.

        Optionally, this module can simulate processing by waiting a configured amount of time (seconds -- specified in the ``delay`` config parameter).
        """

        time.sleep(self.config.get("delay"))  # type: ignore

        output_values: typing.Mapping = self.config.get("outputs")  # type: ignore

        value_dict = {}
        for output_name in self.output_names:
            if output_name not in output_values.keys():
                raise NotImplementedError()
                # v = self.output_schemas[output_name].type_obj.fake_value()
                # value_dict[output_name] = v
            else:
                value_dict[output_name] = output_values[output_name]
        outputs.set_values(**value_dict)
