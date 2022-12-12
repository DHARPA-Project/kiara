# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import structlog
import uuid
from typing import TYPE_CHECKING, Any, Union

from kiara.models.module.operation import Operation
from kiara.operations.included_core_operations.pretty_print import (
    PrettyPrintOperationType,
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

    value = kiara.data_registry.get_value(value=value_id)

    op_type: PrettyPrintOperationType = kiara.operation_registry.get_operation_type("pretty_print")  # type: ignore

    data_type = value.data_type_name
    if data_type not in kiara.data_type_names:
        data_type = "any"

    try:
        op: Union[Operation, None] = op_type.get_operation_for_render_combination(
            source_type=data_type, target_type=target_type
        )
    except Exception as e:

        logger.debug(
            "error.pretty_print",
            source_type=data_type,
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
