# -*- coding: utf-8 -*-
import asyncclick as click

from kiara import Kiara
from kiara.utils.output import rich_print


@click.group(name="type")
@click.pass_context
def type_group(ctx):
    """Information about available value types, and details about them."""


@type_group.command(name="list")
@click.pass_context
def list_types(ctx):
    """List available types (work in progress)."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    for type_name, type in kiara_obj.value_types.items():
        rich_print(f"{type_name}: {type}")
