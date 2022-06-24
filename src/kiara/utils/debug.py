# -*- coding: utf-8 -*-
import orjson
import uuid
from rich import box
from rich.syntax import Syntax
from rich.table import Table
from typing import Any, List, Mapping

from kiara.models.module.manifest import Manifest
from kiara.models.values.value import Value
from kiara.utils import orjson_dumps
from kiara.utils.cli import terminal_print

DEFAULT_VALUE_MAP_RENDER_CONFIG = {
    "ignore_fields": [
        "kiara_id",
        "data_type_class",
        "destiny_backlinks",
        "environments",
        "property_links",
    ],
}


def create_module_preparation_table(
    manifest: Manifest, inputs: Mapping[str, Any], **render_config
):

    table = Table(show_header=False, box=box.SIMPLE)
    table.add_column("key", style="i")
    table.add_column("value")

    table.add_row("module", manifest.module_type)
    if manifest.module_config:
        mc = Syntax(
            orjson_dumps(manifest.module_config, option=orjson.OPT_INDENT_2),
            "json",
            background_color="default",
        )
        table.add_row("module config", mc)
    value_map_rend = create_value_map_renderable(value_map=inputs, **render_config)
    table.add_row("inputs", value_map_rend)

    return table


def terminal_print_manifest(manifest: Manifest):

    terminal_print(manifest.create_renderable())


def create_value_map_renderable(value_map: Mapping[str, Any], **render_config: Any):

    rc = dict(DEFAULT_VALUE_MAP_RENDER_CONFIG)
    rc.update(render_config)

    table = Table(show_header=True, box=box.SIMPLE)
    table.add_column("field name", style="i")
    table.add_column("type")
    table.add_column("value")

    for k, v in value_map.items():
        row: List[Any] = [k]
        if isinstance(v, Value):
            row.append("value object")
            row.append(v.create_renderable(**rc))
        elif isinstance(v, uuid.UUID):
            row.append("value id")
            row.append(str(v))
        else:
            row.append("raw data")
            row.append(str(v))

        table.add_row(*row)

    return table
