# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import TYPE_CHECKING, Dict, Optional

from kiara.models.module.operation import Operation, OperationGroupInfo, OperationInfo

if TYPE_CHECKING:
    from kiara import Kiara


def filter_operations(
    kiara: "Kiara", pkg_name: Optional[str] = None, **operations: "Operation"
) -> OperationGroupInfo:

    result: Dict[str, OperationInfo] = {}

    # op_infos = kiara.operation_registry.get_context_metadata(only_for_package=pkg_name)
    modules = kiara.module_registry.get_context_metadata(only_for_package=pkg_name)

    for op_id, op in operations.items():

        if op.module.module_type_name != "pipeline":
            if op.module.module_type_name in modules.keys():
                result[op_id] = OperationInfo.create_from_operation(
                    kiara=kiara, operation=op
                )
                continue
        else:
            package: Optional[str] = op.metadata.get("labels", {}).get("package", None)
            if package and package == pkg_name:
                result[op_id] = OperationInfo.create_from_operation(
                    kiara=kiara, operation=op
                )

        # opt_types = kiara.operation_registry.find_all_operation_types(op_id)
        # match = False
        # for ot in opt_types:
        #     if ot in op_infos.keys():
        #         match = True
        #         break
        #
        # if match:
        #     result[op_id] = OperationInfo.create_from_operation(
        #         kiara=kiara, operation=op
        #     )

    return OperationGroupInfo.construct(type_infos=result)
