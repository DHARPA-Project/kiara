# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
from typing import TYPE_CHECKING, Any, Dict, Mapping, Union

from kiara.exceptions import NoSuchExecutionTargetException, NoSuchOperationException
from kiara.interfaces.python_api.models.info import OperationGroupInfo, OperationInfo
from kiara.models.module.jobs import ExecutionContext
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import Operation
from kiara.models.module.pipeline import PipelineConfig
from kiara.utils.files import get_data_from_file

if TYPE_CHECKING:
    from kiara.context import Kiara


def filter_operations(
    kiara: "Kiara", pkg_name: Union[str, None] = None, **operations: "Operation"
) -> OperationGroupInfo:

    result: Dict[str, OperationInfo] = {}

    # op_infos = kiara.operation_registry.get_context_metadata(only_for_package=pkg_name)
    modules = kiara.module_registry.get_context_metadata(only_for_package=pkg_name)

    for op_id, op in operations.items():

        if op.module.module_type_name != "pipeline":
            if op.module.module_type_name in modules.item_infos.keys():
                result[op_id] = OperationInfo.create_from_operation(
                    kiara=kiara, operation=op
                )
                continue
        else:
            package: Union[str, None] = op.metadata.get("labels", {}).get(
                "package", None
            )
            if not pkg_name or (package and package == pkg_name):
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

    return OperationGroupInfo.construct(item_infos=result)  # type: ignore


def create_operation(
    module_or_operation: str,
    operation_config: Union[None, Mapping[str, Any]] = None,
    kiara: Union[None, "Kiara"] = None,
) -> Operation:

    operation: Union[Operation, None]

    if kiara is None:
        from kiara.context import Kiara

        kiara = Kiara.instance()

    if module_or_operation in kiara.operation_registry.operation_ids:

        operation = kiara.operation_registry.get_operation(module_or_operation)
        if operation_config:
            raise Exception(
                f"Specified run target '{module_or_operation}' is an operation, additional module configuration is not allowed."
            )

    elif module_or_operation in kiara.module_type_names:

        manifest = Manifest(
            module_type=module_or_operation, module_config=operation_config
        )
        module = kiara.create_module(manifest=manifest)
        operation = Operation.create_from_module(module)

    elif os.path.isfile(module_or_operation):
        data = get_data_from_file(module_or_operation)
        pipeline_name = data.pop("pipeline_name", None)
        if pipeline_name is None:
            pipeline_name = os.path.basename(module_or_operation)

        # self._defaults = data.pop("inputs", {})

        execution_context = ExecutionContext(
            pipeline_dir=os.path.abspath(os.path.dirname(module_or_operation))
        )
        pipeline_config = PipelineConfig.from_config(
            pipeline_name=pipeline_name,
            data=data,
            kiara=kiara,
            execution_context=execution_context,
        )

        manifest = kiara.create_manifest("pipeline", config=pipeline_config.dict())
        module = kiara.create_module(manifest=manifest)

        operation = Operation.create_from_module(module, doc=pipeline_config.doc)

    else:
        raise NoSuchOperationException(
            msg=f"Can't assemble operation, invalid operation/module name: {module_or_operation}. Must be registered module or operation name, or file.",
            operation_id=module_or_operation,
            available_operations=sorted(kiara.operation_registry.operation_ids),
        )
        # manifest = Manifest(
        #     module_type=module_or_operation,
        #     module_config=self._operation_config,
        # )
        # module = self._kiara.create_module(manifest=manifest)
        # operation = Operation.create_from_module(module=module)

    if operation is None:

        merged = set(kiara.module_type_names)
        merged.update(kiara.operation_registry.operation_ids)
        raise NoSuchExecutionTargetException(
            selected_target=module_or_operation,
            msg=f"Invalid run target name '{module_or_operation}'. Must be a path to a pipeline file, or one of the available modules/operations.",
            available_targets=sorted(merged),
        )
    return operation
