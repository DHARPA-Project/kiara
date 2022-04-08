# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Type-related subcommands for the cli."""
import orjson.orjson
import rich_click as click
from rich.panel import Panel
from typing import Iterable

from kiara import Kiara
from kiara.models.values.data_type import DataTypeClassesInfo, DataTypeClassInfo
from kiara.utils import rich_print
from kiara.utils.graphs import print_ascii_graph


@click.group(name="data-type")
@click.pass_context
def type_group(ctx):
    """Information about available value data_types, and details about them."""


@type_group.command(name="list")
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full documentation for every data type (when using 'terminal' output format).",
)
@click.option(
    "--include-internal-types",
    "-i",
    is_flag=True,
    help="Also list types that are only (or mostly) used internally.",
)
@click.argument("filter", nargs=-1, required=False)
@click.option(
    "--format",
    "-f",
    help="The output format. Defaults to 'terminal'.",
    type=click.Choice(["terminal", "json", "html"]),
    default="terminal",
)
@click.pass_context
def list_types(
    ctx, full_doc, include_internal_types: bool, filter: Iterable[str], format: str
):
    """List available data_types (work in progress)."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    if not include_internal_types:
        type_classes = {}
        for name, cls in kiara_obj.data_type_classes.items():
            lineage = kiara_obj.type_registry.get_type_lineage(name)
            if "any" in lineage:
                type_classes[name] = cls
    else:
        type_classes = kiara_obj.data_type_classes

    title = "Available data types"
    if filter:
        title = "Filtered data types"
        temp = {}
        for k, v in type_classes.items():
            match = True
            for f in filter:
                if f.lower() not in k.lower():
                    match = False
                    break
            if match:
                temp[k] = v
        type_classes = temp

    data_types_info = DataTypeClassesInfo.create_from_items(
        group_alias=title, **type_classes
    )

    if format == "terminal":
        print()
        p = Panel(
            data_types_info.create_renderable(full_doc=full_doc),
            title_align="left",
            title="Available data types",
        )
        rich_print(p)
    elif format == "json":
        print(data_types_info.json(option=orjson.orjson.OPT_INDENT_2))
    elif format == "html":
        print(data_types_info.create_html())


@type_group.command(name="hierarchy")
@click.option("--details", "-d", is_flag=True, help="Display full description.")
@click.pass_context
def hierarchy(ctx, details):
    """List available data_types (work in progress)."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    type_mgmt = kiara_obj.type_registry
    print()

    print_ascii_graph(type_mgmt.data_type_hierarchy)


@type_group.command(name="explain")
@click.argument("data_type", nargs=1, required=True)
@click.pass_context
def explain_data_type(ctx, data_type: str):
    """Print details of a module type.

    This is different to the 'explain-instance' command, because module data_types need to be
    instantiated with configuration, before we can query all their properties (like
    input/output data_types).
    """

    kiara_obj: Kiara = ctx.obj["kiara"]

    dt_cls = kiara_obj.type_registry.get_data_type_cls(data_type)
    info = DataTypeClassInfo.create_from_type_class(dt_cls)

    rich_print()
    rich_print(info.create_panel(title=f"Value type: [b i]{data_type}[/b i]"))
