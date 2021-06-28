# -*- coding: utf-8 -*-
"""Type-related subcommands for the cli."""
import asyncclick as click

from kiara import Kiara
from kiara.data.types import ValueTypesInfo
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
