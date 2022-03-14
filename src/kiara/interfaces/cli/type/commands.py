# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Type-related subcommands for the cli."""

import rich_click as click

from kiara import Kiara
from kiara.models.values.value_type import ValueTypeClassesInfo, ValueTypeClassInfo
from kiara.utils.output import rich_print


@click.group(name="type")
@click.pass_context
def type_group(ctx):
    """Information about available value value_types, and details about them."""


@type_group.command(name="list")
@click.option("--details", "-d", is_flag=True, help="Display full description.")
@click.pass_context
def list_types(ctx, details):
    """List available value_types (work in progress)."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value_types_info = ValueTypeClassesInfo(
        kiara_obj.value_type_classes, id="all_types", details=details
    )

    rich_print(value_types_info)


@type_group.command(name="hierarchy")
@click.option("--details", "-d", is_flag=True, help="Display full description.")
@click.pass_context
def hierarchy(ctx, details):
    """List available value_types (work in progress)."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    type_mgmt = kiara_obj.type_mgmt
    print()

    print(type_mgmt.value_type_hierarchy)
    print_ascii_graph(type_mgmt.value_type_hierarchy)


@type_group.command(name="explain")
@click.argument("type", nargs=1, required=True)
@click.pass_context
def explain_module_type(ctx, value_type: str):
    """Print details of a module type.

    This is different to the 'explain-instance' command, because module value_types need to be
    instantiated with configuration, before we can query all their properties (like
    input/output value_types).
    """

    kiara_obj: Kiara = ctx.obj["kiara"]

    vt_cls = kiara_obj.type_mgmt.get_value_type_cls(value_type)
    info = ValueTypeClassInfo.create_from_value_type(vt_cls)

    rich_print()
    rich_print(info.create_panel(title=f"ValueOrm type: [b i]{value_type}[/b i]"))
