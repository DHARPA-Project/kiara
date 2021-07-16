# -*- coding: utf-8 -*-
from pydantic import Field

from kiara.data.operations import OperationType


class SaveOperationType(OperationType):

    # @classmethod
    # def retrieve_operation_configs(
    #     cls, kiara: "Kiara"
    # ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
    #
    #     result = {}
    #     for value_type, value_type_cls in kiara.value_types.items():
    #         save_config = value_type_cls.save_config()
    #         if not save_config:
    #             continue
    #
    #         result.setdefault(value_type, {}).setdefault("save_value", {})[
    #             "data_store"
    #         ] = save_config
    #
    #     return result

    target_name: str = Field(description="The name of the input to specify the target.")
