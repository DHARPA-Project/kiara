# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.data.values import ValueSchema
from kiara.module_config import ModuleTypeConfigSchema


class ValueMergeModuleConfig(ModuleTypeConfigSchema):

    input_schemas: typing.Dict[str, typing.Mapping[str, typing.Any]] = Field(
        description="The schemas for all of the expected inputs."
    )
    output_type: str = Field(description="The result type of the merged value.")


class ValueMergeModule(KiaraModule):
    """Base class for operations that merge several values into one.

    NOT USED YET.
    """

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        input_schema_dicts: typing.Mapping[
            str, typing.Mapping[str, typing.Any]
        ] = self.get_config_value("input_schemas")
        if not input_schema_dicts:
            raise Exception("No input schemas provided.")

        input_schemas: typing.Dict[str, ValueSchema] = {}
        for k, v in input_schema_dicts.items():
            input_schemas[k] = ValueSchema(**v)

        return input_schemas

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"merged_value": {"type": self.get_config_value("output_type")}}
