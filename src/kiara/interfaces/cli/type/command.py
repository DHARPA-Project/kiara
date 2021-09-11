# -*- coding: utf-8 -*-
"""Type-related subcommands for the cli."""
import click

from kiara import Kiara
from kiara.data.types import ValueTypesInfo
from kiara.info.types import ValueTypeInfo
from kiara.utils.output import rich_print


@click.group(name="type")
@click.pass_context
def type_group(ctx):
    """Information about available value types, and details about them."""


@type_group.command(name="list")
@click.option("--details", "-d", is_flag=True, help="Display full description.")
@click.pass_context
def list_types(ctx, details):
    """List available types (work in progress)."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value_types_info = ValueTypesInfo(kiara_obj.value_types, details=details)

    rich_print(value_types_info)


@type_group.command(name="explain")
@click.argument("value_type", nargs=1, required=True)
@click.pass_context
def explain_module_type(ctx, value_type: str):
    """Print details of a module type.

    This is different to the 'explain-instance' command, because module types need to be
    instantiated with configuration, before we can query all their properties (like
    input/output types).
    """

    kiara_obj: Kiara = ctx.obj["kiara"]

    vt_cls = kiara_obj.type_mgmt.get_value_type_cls(value_type)
    info = ValueTypeInfo.from_type_class(vt_cls, kiara=kiara_obj)

    rich_print()
    rich_print(info.create_panel(title=f"Value type: [b i]{value_type}[/b i]"))
