# -*- coding: utf-8 -*-
"""Data-related sub-commands for the cli."""
import asyncclick as click
import shutil
import typing
from rich import box
from rich.table import Table

from kiara import Kiara
from kiara.data.values import Value
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.utils import is_develop
from kiara.utils.output import rich_print


@click.group()
@click.pass_context
def data(ctx):
    """Data-related sub-commands."""


@data.command(name="list")
@click.pass_context
def list_values(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    table = Table(box=box.SIMPLE)
    table.add_column("id", style="i")
    table.add_column("aliases")
    table.add_column("type")

    print()
    for v_id, d in kiara_obj.data_store.values_metadata.items():

        value_type = kiara_obj.data_store.get_value_type(value_id=v_id)
        aliases = kiara_obj.data_store.get_aliases_for_id(v_id)
        if not aliases:
            aliases.append("")

        table.add_row(v_id, aliases[0], value_type)

        for a in aliases[1:]:
            table.add_row("", a, "")

    rich_print(table)

    # else:
    #     for alias, v_id in kiara_obj.data_store.aliases.items():
    #         v_type = kiara_obj.data_store.get_value_type(v_id)
    #         if not details:
    #             rich_print(f"  - [b]{alias}[/b]: {v_type}")
    #         else:
    #             rich_print(f"[b]{alias}[/b]: {v_type}\n")
    #             md = kiara_obj.data_store.get_value_metadata(value_id=v_id)
    #             s = Syntax(json.dumps(md, indent=2), "json")
    #             rich_print(s)
    #             print()


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


if is_develop():

    @data.command(name="clear-data-store")
    @click.pass_context
    def clean_data_store(ctx):

        kiara_obj: Kiara = ctx.obj["kiara"]

        path = kiara_obj.data_store.data_store_dir
        print()
        print(f"Deleting folder: {path}...")
        shutil.rmtree(path=path, ignore_errors=True)
        print("Folder deleted.")
