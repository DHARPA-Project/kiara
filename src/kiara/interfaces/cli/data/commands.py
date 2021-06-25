# -*- coding: utf-8 -*-
"""Data-related sub-commands for the cli."""

import asyncclick as click
import json
import typing
from rich.syntax import Syntax

from kiara import Kiara
from kiara.data.values import Value
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.utils.output import rich_print


@click.group()
@click.pass_context
def data(ctx):
    """Data-related sub-commands."""


@data.command(name="list")
@click.option("--details", "-d", help="Display data item details.", is_flag=True)
@click.option("--ids", "-i", help="List value ids instead of aliases.", is_flag=True)
@click.pass_context
def list_values(ctx, details, ids):

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    if ids:
        for id, d in kiara_obj.data_store.values_metadata.items():
            if not details:
                rich_print(f"  - [b]{id}[/b]: {d['type']}")
            else:
                rich_print(f"[b]{id}[/b]: {d['type']}\n")
                md = kiara_obj.data_store.get_value_metadata(value_id=id)
                s = Syntax(json.dumps(md, indent=2), "json")
                rich_print(s)
                print()
    else:
        for alias, v_id in kiara_obj.data_store.aliases.items():
            v_type = kiara_obj.data_store.get_value_type(v_id)
            if not details:
                rich_print(f"  - [b]{alias}[/b]: {v_type}")
            else:
                rich_print(f"[b]{alias}[/b]: {v_type}\n")
                md = kiara_obj.data_store.get_value_metadata(value_id=v_id)
                s = Syntax(json.dumps(md, indent=2), "json")
                rich_print(s)
                print()


@data.command(name="explain")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def explain_value(ctx, value_id: str):

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value = kiara_obj.data_store.load_value(value_id=value_id)
    rich_print(value)


@data.command(name="load")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def load_value(ctx, value_id: str):

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value = kiara_obj.data_store.load_value(value_id=value_id)

    pretty_print_config: typing.Dict[str, typing.Any] = {"item": value}
    pretty_print_config.update(DEFAULT_PRETTY_PRINT_CONFIG)
    renderables: Value = kiara_obj.run(  # type: ignore
        "string.pretty_print", inputs=pretty_print_config, output_name="renderables"
    )
    rich_print(*renderables.get_value_data())
