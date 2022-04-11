# -*- coding: utf-8 -*-
import structlog
import uuid
from rich import box
from rich.table import Table
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Union

from kiara.models.values.value import Value
from kiara.utils import rich_print

if TYPE_CHECKING:
    from kiara.kiara import Kiara

logger = structlog.getLogger()


def render_data(
    kiara: "Kiara",
    value_id: uuid.UUID,
    target_type="terminal_renderable",
    **render_config: Any,
) -> Any:

    value = kiara.data_registry.get_value(value_id=value_id)

    op_type: RenderValueOperationType = kiara.operation_registry.get_operation_type("render_value")  # type: ignore

    try:
        op = op_type.get_operation_for_render_combination(
            source_type=value.value_schema.type, target_type=target_type
        )
    except Exception as e:

        logger.debug(
            "error.render_value",
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

    assert op is not None
    result = op.run(kiara=kiara, inputs={"value": value})
    rendered = result.get_value_data("rendered_value")
    return rendered


def render_value_list(
    kiara: "Kiara",
    values: Iterable[Union[Value, uuid.UUID]],
    render_config: Mapping[str, Any] = None,
):

    if render_config is None:
        render_config = {}

    show_aliases = render_config.get("show_aliases", True)
    show_ids = render_config.get("show_ids", False)

    values = (
        v if isinstance(v, Value) else kiara.data_registry.get_value(v) for v in values
    )

    properties = ["id", "aliases"]

    table = Table(show_lines=True, show_header=True, box=box.SIMPLE)

    for property in properties:
        table.add_column(property)

    for value in values:
        row = []

        for property in properties:

            if property == "id":
                row.append(str(value.value_id))
            if property == "aliases":
                aliases = kiara.alias_registry.find_aliases_for_value_id(value.value_id)
                row.append(", ".join(aliases))
        table.add_row(*row)

    rich_print(table)
