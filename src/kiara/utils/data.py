# -*- coding: utf-8 -*-
import structlog
import uuid
from rich import box
from rich.table import Table
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Union

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


def create_values_info_model(
    kiara: "Kiara",
    values: Iterable[Union[Value, uuid.UUID]],
    fields: Optional[Iterable[str]] = None,
    sort_by_alias: bool = False,
):

    all_properties = [
        "value_schema",
        "properties",
        "aliases",
        "is_stored",
        "pedigree",
        "load_config",
    ]

    if fields is None:
        fields = all_properties

    models = []

    for value in values:

        if isinstance(value, uuid.UUID):
            value = kiara.data_registry.get_value(value)

        info = {"id": value.value_id, "is_stored": value.is_stored}
        for property in fields:
            # if property == "properties":
            #     prop_model = create_dynamic_properties_model(kiara=kiara, properties=value.property_values)
            if property == "aliases":
                aliases = kiara.alias_registry.find_aliases_for_value_id(value.value_id)
                info["aliases"] = aliases
            if property == "value_schema":
                info["value_schema"] = value.value_schema
            if property == "pedigree":
                info["pedigree"] = value.pedigree

        model = ValueInfoModel(**info)
        models.append(model)

    return ValuesInfoModel(__root__=models)


def render_value_list(
    kiara: "Kiara",
    values: Iterable[Union[Value, uuid.UUID]],
    render_config: Mapping[str, Any] = None,
):

    if render_config is None:
        render_config = {}

    show_aliases = render_config.get("show_aliases", True)
    show_ids = render_config.get("show_ids", False)
    show_type = render_config.get("show_type", True)

    if not show_aliases:
        by_aliases = False
    else:
        by_aliases = render_config.get("by_aliases", True)

    values = (
        v if isinstance(v, Value) else kiara.data_registry.get_value(v) for v in values
    )

    properties = ["aliases", "id", "type", "properties"]

    table = Table(show_lines=False, show_header=True, box=box.SIMPLE)

    for property in properties:
        table.add_column(property)

    rich_print(table)
