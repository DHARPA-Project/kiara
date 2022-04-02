# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Type-related subcommands for the cli."""

import rich_click as click

from kiara import Kiara
from kiara.models.values.data_type import DataTypeClassesInfo, ValueTypeClassInfo
from kiara.utils import rich_print
from kiara.utils.graphs import print_ascii_graph


@click.group(name="data-type")
@click.pass_context
def type_group(ctx):
    """Information about available value data_types, and details about them."""


@type_group.command(name="list")
@click.option("--details", "-d", is_flag=True, help="Display full description.")
@click.option(
    "--include-internal-types",
    "-i",
    is_flag=True,
    help="Also list types that are only (or mostly) used internally.",
)
@click.pass_context
def list_types(ctx, details, include_internal_types: bool):
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

    print()
    data_types_info = DataTypeClassesInfo(type_classes, id="all_types", details=details)

    rich_print(data_types_info)


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
    info = ValueTypeClassInfo.create_from_data_type(dt_cls)

    rich_print()
    rich_print(info.create_panel(title=f"ValueOrm type: [b i]{data_type}[/b i]"))
