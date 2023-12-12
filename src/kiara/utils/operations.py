# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Union

from rich.console import Group, RenderableType
from rich.markdown import Markdown
from ruamel.yaml import YAML

from kiara.exceptions import (
    InvalidValuesException,
    NoSuchExecutionTargetException,
    NoSuchOperationException,
)
from kiara.interfaces.python_api.models.info import OperationGroupInfo, OperationInfo
from kiara.models.module.jobs import ExecutionContext
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import Operation
from kiara.models.module.pipeline import PipelineConfig
from kiara.utils.files import get_data_from_file
from kiara.utils.output import (
    create_table_from_field_schemas,
    create_value_map_status_renderable,
)

if TYPE_CHECKING:
    from kiara.api import ValueMap
    from kiara.context import Kiara

yaml = YAML(typ="safe")


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

    return OperationGroupInfo(item_infos=result)  # type: ignore


def create_operation(
    module_or_operation: str,
    operation_config: Union[None, Mapping[str, Any]] = None,
    kiara: Union[None, "Kiara"] = None,
) -> Operation:

    operation: Union[Operation, None]

    if kiara is None:
        from kiara.context import Kiara

        kiara = Kiara.instance()

    operation = None

    if module_or_operation in kiara.operation_registry.operation_ids:

        operation = kiara.operation_registry.get_operation(module_or_operation)
        if operation_config:
            if module_or_operation in kiara.module_type_names:
                manifest = Manifest(
                    module_type=module_or_operation, module_config=operation_config
                )
                module = kiara.module_registry.create_module(manifest=manifest)
                operation = Operation.create_from_module(module)
            else:
                raise Exception(
                    f"Specified run target '{module_or_operation}' is an operation, additional module configuration is not allowed."
                )

    elif (
        module_or_operation != "pipeline"
        and module_or_operation in kiara.module_type_names
    ):

        if operation_config is None:
            operation_config = {}
        manifest = Manifest(
            module_type=module_or_operation, module_config=operation_config
        )
        module = kiara.module_registry.create_module(manifest=manifest)
        operation = Operation.create_from_module(module)

    elif os.path.isfile(module_or_operation):
        _data = get_data_from_file(module_or_operation)
        pipeline_name = _data.pop("pipeline_name", None)
        if pipeline_name is None:
            pipeline_name = os.path.basename(module_or_operation)

        # self._defaults = data.pop("inputs", {})

        execution_context = ExecutionContext(
            pipeline_dir=os.path.abspath(os.path.dirname(module_or_operation))
        )
        pipeline_config = PipelineConfig.from_config(
            pipeline_name=pipeline_name,
            data=_data,
            kiara=kiara,
            execution_context=execution_context,
        )

        manifest = kiara.create_manifest(
            "pipeline", config=pipeline_config.model_dump()
        )
        module = kiara.module_registry.create_module(manifest=manifest)

        operation = Operation.create_from_module(module, doc=pipeline_config.doc)

    else:
        if module_or_operation == "pipeline":
            data: Union[None, Mapping[str, Any]] = operation_config
        else:
            try:
                import json

                data = json.loads(module_or_operation)
            except Exception:
                try:
                    data = yaml.load(module_or_operation)
                except Exception:
                    data = None

            if data and not isinstance(data, Mapping):
                raise Exception(
                    f"Could not parse module or operation: {module_or_operation}"
                )

        if data:
            d = dict(data)
            pipeline_name = d.pop("pipeline_name", None)
            if pipeline_name is not None:

                execution_context = ExecutionContext(
                    pipeline_dir=os.path.abspath(os.path.dirname(module_or_operation))
                )
                pipeline_config = PipelineConfig.from_config(
                    pipeline_name=pipeline_name,
                    data=d,
                    kiara=kiara,
                    execution_context=execution_context,
                )

                manifest = kiara.create_manifest(
                    "pipeline", config=pipeline_config.model_dump()
                )
                module = kiara.module_registry.create_module(manifest=manifest)

                operation = Operation.create_from_module(
                    module, doc=pipeline_config.doc
                )
            else:
                raise Exception("Invalid pipeline config, missing 'pipeline_name' key.")

        if operation is None:

            if module_or_operation == "pipeline":
                msg = "Can't assemble pipeline."
            else:
                msg = f"Can't assemble operation, invalid operation/module name: {module_or_operation}. Must be registered module or operation name, or file."
            raise NoSuchOperationException(
                msg=msg,
                operation_id=module_or_operation,
                available_operations=sorted(kiara.operation_registry.operation_ids),
            )

    if operation is None:

        merged = set(kiara.module_type_names)
        merged.update(kiara.operation_registry.operation_ids)
        raise NoSuchExecutionTargetException(
            selected_target=module_or_operation,
            msg=f"Invalid run target name '{module_or_operation}'. Must be a path to a pipeline file, or one of the available modules/operations.",
            available_targets=sorted(merged),
        )
    return operation


def create_operation_status_renderable(
    operation: Operation, inputs: Union["ValueMap", None], render_config: Any
) -> RenderableType:

    show_operation_name = render_config.get("show_operation_name", True)
    show_operation_doc = render_config.get("show_operation_doc", True)
    show_only_description = render_config.get("show_only_description", True)
    show_inputs = render_config.get("show_inputs", False)
    show_outputs_schema = render_config.get("show_outputs_schema", False)
    show_headers = render_config.get("show_headers", True)

    items: List[Any] = []

    if show_operation_name:
        items.append(f"Operation: [bold]{operation.operation_id}[/bold]")
    if show_operation_doc and operation.doc.is_set:
        items.append("")
        if show_only_description:
            items.append(Markdown(operation.doc.description))
        else:
            items.append(Markdown(operation.doc.full_doc))

    if show_inputs:
        assert inputs is not None
        if show_headers:
            items.append("\nInputs:")
        _inputs: Union[None, RenderableType] = None
        try:
            _inputs = create_value_map_status_renderable(
                inputs, render_config=render_config
            )
        except InvalidValuesException as ive:
            _inputs = ive.create_renderable(**render_config)
        except Exception as e:
            _inputs = f"[red bold]{e}[/red bold]"
        finally:
            assert _inputs is not None
            items.append(_inputs)
    if show_outputs_schema:
        if show_headers:
            items.append("\nOutputs:")
        outputs_schema = create_table_from_field_schemas(
            _add_default=False,
            _add_required=False,
            _show_header=True,
            _constants=None,
            fields=operation.outputs_schema,
        )
        items.append(outputs_schema)

    return Group(*items)
