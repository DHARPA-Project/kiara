# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.data.values import ValueSchema, ValueSet
from kiara.module_config import KiaraModuleConfig


class SaveValueModuleConfig(KiaraModuleConfig):

    value_type: str = Field(description="The type of the value to save.")


class SaveValueModule(KiaraModule):
    """Save a value into the kiara data store."""

    _type_name = "value.save"
    _config_cls = SaveValueModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "value_item": {"type": "any", "doc": "The value to save."},
            "aliases": {
                "type": "list",
                "doc": "A list of aliases to link to the saved value id.",
                "optional": True,
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"value_id": {"type": "string", "doc": "The id of the saved value."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value = inputs.get_value_obj("value_item")
        aliases = inputs.get_value_data("aliases")

        value_id = self._kiara.data_store.save_value(
            value=value, aliases=aliases, value_type=self.get_config_value("value_type")
        )

        outputs.set_value("value_id", value_id)
