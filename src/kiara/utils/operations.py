# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from kiara import Kiara
    from kiara.models.module.operation import Operation


def filter_operations_for_package(
    kiara: "Kiara", pkg_name: str, **operations: "Operation"
) -> Dict[str, "Operation"]:

    result = {}

    op_infos = kiara.operation_registry.get_context_metadata(only_for_package=pkg_name)
    modules = kiara.module_registry.get_context_metadata(only_for_package=pkg_name)

    for op_id, op in operations.items():

        if op.module in modules:
            result[op_id] = op
            continue

        opt_types = kiara.operation_registry.find_all_operation_types(op_id)
        match = False
        for ot in opt_types:
            if ot in op_infos.keys():
                match = True
                break

        if match:
            result[op_id] = op

    return result
