# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import structlog
import uuid
from typing import TYPE_CHECKING, Any, Optional

from kiara.models.module.operation import Operation
from kiara.models.render_value import RenderInstruction, RenderValueResult
from kiara.operations.included_core_operations.pretty_print import (
    PrettyPrintOperationType,
)
from kiara.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()


def pretty_print_data(
    kiara: "Kiara",
    value_id: uuid.UUID,
    target_type="terminal_renderable",
    **render_config: Any,
) -> Any:

    value = kiara.data_registry.get_value(value_id=value_id)

    op_type: PrettyPrintOperationType = kiara.operation_registry.get_operation_type("pretty_print")  # type: ignore

    try:
        op: Optional[Operation] = op_type.get_operation_for_render_combination(
            source_type=value.value_schema.type, target_type=target_type
        )
    except Exception as e:

        logger.debug(
            "error.pretty_print",
            source_type=value.value_schema.type,
            target_type=target_type,
            error=e,
        )

        op = None
        if target_type == "terminal_renderable":
            try:
                op = op_type.get_operation_for_render_combination(
                    source_type="any", target_type="string"
                )
            except Exception:
                pass

    if op is None:
        raise Exception(
            f"Can't find operation to render '{value.value_schema.type}' as '{target_type}."
        )

    result = op.run(kiara=kiara, inputs={"value": value})
    rendered = result.get_value_data("rendered_value")
    return rendered


def render_value(
    kiara: "Kiara",
    value_id: uuid.UUID,
    target_type="terminal_renderable",
    render_instruction: Optional[RenderInstruction] = None,
) -> RenderValueResult:

    value = kiara.data_registry.get_value(value_id=value_id)
    op_type: RenderValueOperationType = kiara.operation_registry.get_operation_type("render_value")  # type: ignore

    ops = op_type.get_render_operations_for_source_type(value.data_type_name)
    if target_type not in ops.keys():
        if not ops:
            msg = f"No render operations registered for source type '{value.data_type_name}'."
        else:
            msg = f"Registered target types for source type '{value.data}': {', '.join(ops.keys())}."
        raise Exception(
            f"No render operation for source type '{value.data_type_name}' to target type '{target_type}' registered. {msg}"
        )

    op = ops[target_type]
    result = op.run(
        kiara=kiara, inputs={"value": value, "render_instruction": render_instruction}
    )

    return RenderValueResult(
        rendered=result.get_value_data("rendered_value"),
        metadata=result.get_value_data("render_metadata"),
    )
