# -*- coding: utf-8 -*-
import time
import typing
from pydantic import Field

from kiara.config import KiaraModuleConfig
from kiara.data.values import ValueSchema, ValueType
from kiara.module import KiaraModule, StepInputs, StepOutputs


class LogicProcessingModuleConfig(KiaraModuleConfig):
    """Config class for all the 'logic'-related modules."""

    delay: float = Field(
        default=0,
        description="the delay in seconds from processing start to when the output is returned.",
    )


class LogicProcessingModule(KiaraModule):

    _config_cls = LogicProcessingModuleConfig


class NotModule(LogicProcessingModule):
    """Negates the input."""

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:
        """The not module only has one input, a boolean that will be negated by the module."""

        return {
            "a": ValueSchema(
                type=ValueType.boolean, doc="A boolean describing this input state."
            ),
        }

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        """The output of this module is a single boolean, the negated input."""

        return {
            "y": ValueSchema(
                type=ValueType.boolean,
                doc="A boolean describing the module output state.",
            )
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:
        """Negates the input boolean."""

        time.sleep(self.config.get("delay"))  # type: ignore

        outputs.y = not inputs.a


class AndModule(LogicProcessingModule):
    """Returns 'True' if both inputs are 'True'."""

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:

        return {
            "a": ValueSchema(
                type=ValueType.boolean, doc="A boolean describing this input state."
            ),
            "b": ValueSchema(
                type=ValueType.boolean, doc="A boolean describing this input state."
            ),
        }

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:

        return {
            "y": ValueSchema(
                type=ValueType.boolean,
                doc="A boolean describing the module output state.",
            )
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        time.sleep(self.config.delay)  # type: ignore

        outputs.y = inputs.a and inputs.b


class OrModule(LogicProcessingModule):
    """Returns 'True' if one of the inputs is 'True'."""

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:

        return {
            "a": ValueSchema(
                type=ValueType.boolean, doc="A boolean describing this input state."
            ),
            "b": ValueSchema(
                type=ValueType.boolean, doc="A boolean describing this input state."
            ),
        }

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:

        return {
            "y": ValueSchema(
                type=ValueType.boolean,
                doc="A boolean describing the module output state.",
            )
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        time.sleep(self.config.get("delay"))  # type: ignore
        outputs.y = inputs.a or inputs.b
