# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara.data import ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module import KiaraModule
from kiara.module_config import ModuleTypeConfigSchema

if typing.TYPE_CHECKING:
    from kiara.data.values import ValueSchema


class SaveValueModuleConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The type of the value to save.")


class SaveValueModule(KiaraModule):
    """Save a value into the kiara data store."""

    _type_name = "value.save"
    _config_cls = SaveValueModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union["ValueSchema", typing.Mapping[str, typing.Any]]
    ]:

        return {
            "value_item": {
                "type": self.get_config_value("value_type"),
                "doc": "The value to save.",
            },
            "aliases": {
                "type": "list",
                "doc": "A list of aliases to link to the saved value id.",
                "optional": True,
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union["ValueSchema", typing.Mapping[str, typing.Any]]
    ]:
        return {
            "value_id": {
                "type": "string",
                "doc": f"The id of the saved {self.get_config_value('value_type')} data-item.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value = inputs.get_value_obj("value_item")
        aliases = inputs.get_value_data("aliases")

        assert value.type_name == self.get_config_value("value_type")

        value = value.save(aliases=aliases)

        outputs.set_value("value_id", value.id)


class LoadValueModule(KiaraModule):
    """Load a value from the kiara data store."""

    _type_name = "value.load"
    _config_cls = SaveValueModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union["ValueSchema", typing.Mapping[str, typing.Any]]
    ]:

        return {
            "value_id": {
                "type": "string",
                "doc": "The id or alias of the saved value you want to load.",
            }
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union["ValueSchema", typing.Mapping[str, typing.Any]]
    ]:

        return {
            "value_item": {
                "type": self.get_config_value("value_type"),
                "doc": "The loaded value.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value_id: str = inputs.get_value_data("value_id")  # type: ignore

        value = self._kiara.data_store.get_value_obj(value_id)
        if value is None:
            raise KiaraProcessingException(f"Can't find value for value id: {value_id}")
        # TODO: make this so we don't have to actually load the data, but can use a reference
        outputs.set_value("value_item", value.get_value_data())
