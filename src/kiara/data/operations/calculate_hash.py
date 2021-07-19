# -*- coding: utf-8 -*-
import typing

from kiara.data.operations import OperationType

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


class CalculateHashOperationType(OperationType):
    """Calculate a hash for a dataset."""

    @classmethod
    def retrieve_operation_configs(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]:

        result: typing.Dict[str, typing.Dict[str, typing.Dict[str, typing.Any]]] = {}
        for value_type, operation_type_cls in kiara.value_types.items():

            result.setdefault(value_type, {}).setdefault("calculate_hash", {})[
                "default"
            ] = {
                "module_type": "metadata.value_hash",
                "module_config": {"value_type": value_type},
                "input_name": "value_item",
            }

        return result
