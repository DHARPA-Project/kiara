# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Data-related sub-commands for the cli."""

import rich_click as click
import shutil
import sys
from rich import box
from rich.console import RenderGroup
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from kiara import Kiara
from kiara.utils import StringYAML, is_debug, is_develop
from kiara.utils.output import rich_print

yaml = StringYAML()


@click.group()
@click.pass_context
def data(ctx):
    """Data-related sub-commands."""


@data.command(name="list")
@click.option(
    "--with-alias/--all-ids",
    help="Also list values without aliases (default: '--with-alias').",
    is_flag=True,
    default=True,
)
@click.option(
    "--only-latest/--all-versions",
    help="List all alias only_latest, not just the latest (default: '--only-latest').",
    is_flag=True,
    default=True,
)
@click.option(
    "--tags/--no-tags",
    help="List alias tags (default: '--tags').",
    is_flag=True,
    default=True,
)
@click.option(
    "--all",
    "-a",
    help="Display all information and values. Overrides the other options.",
    is_flag=True,
)
@click.option(
    "--show-pedigree",
    "-p",
    help="Display pedigree information for each value.",
    default=False,
    is_flag=True
)
@click.option(
    "--show-data",
    "-d",
    help="Show a preview of the data associated with this value.",
    default=False,
    is_flag=True
)
@click.pass_context
def list_values(ctx, with_alias, only_latest, tags, all, show_pedigree, show_data):
    """List all data items that are stored in kiara."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    table = kiara_obj.data_store
    rich_print(table, show_pedigree=show_pedigree, show_data=show_data)


@data.command(name="explain")
@click.argument("value_id", nargs=-1, required=True)
@click.option(
    "--no-metadata", "-nm", help="Don't display value metadata.", is_flag=True
)
@click.option(
    "--lineage", "-l", help="Display lineage information for the value.", is_flag=True
)
@click.option(
    "--include-ids", "-i", help="Include ids in lineage display.", is_flag=True
)
@click.pass_context
def explain_value(
    ctx, value_id: str, no_metadata: bool, lineage: bool, include_ids: bool
):
    """Print the metadata of a stored value."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    for v_id in value_id:
        value = kiara_obj.data_store.retrieve_value(v_id)
        print()
        if not value:
            print(f"No saved value found for: {v_id}")
            continue
        table = value.create_renderable(
            skip_metadata=no_metadata, skip_lineage=not lineage, include_ids=include_ids
        )
        rich_print(
            Panel(
                table,
                box=box.ROUNDED,
                title_align="left",
                title=f"Value: [b]{v_id}[/b]",
            )
        )


# @data.command(name="explain-lineage")
# @click.argument("value_id", nargs=1, required=True)
# @click.pass_context
# def explain_lineage(ctx, value_id: str):
#
#     kiara_obj: Kiara = ctx.obj["kiara"]
#
#     value = kiara_obj.data_store.get_value_obj(value_item=value_id)
#     if value is None:
#         print(f"No value stored for: {value_id}")
#         sys.exit(1)
#
#     value_info = value.create_info()
#
#     lineage = value_info.lineage
#     if not lineage:
#         print(f"No lineage information associated to value '{value_id}'.")
#         sys.exit(0)
#
#     yaml_str = yaml.dump(lineage.to_minimal_dict())
#     syntax = Syntax(yaml_str, "yaml", background_color="default")
#     rich_print(
#         Panel(
#             syntax,
#             title=f"Lineage for: {value_id}",
#             title_align="left",
#             box=box.ROUNDED,
#             padding=(1, 0, 0, 2),
#         )
#     )


@data.command(name="load")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def load_value(ctx, value_id: str):
    """Load a stored value and print it in a format suitable for the terminal."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    value = kiara_obj.data_store.retrieve_value(value_id=value_id)
    if value is None:
        print(f"No value available for id: {value_id}")
        sys.exit(1)

    try:
        renderables = kiara_obj.type_mgmt.render_value(value, render_target="terminal")
    except Exception as e:

        if is_debug():
            print(e)
        renderables = [str(value.data)]

    if len(renderables) == 0:
        return
    elif len(renderables) == 1:
        print()
        rich_print(renderables[0])
    else:
        rg = RenderGroup(*renderables)
        print()
        rich_print(rg)


if is_develop():

    @data.command(name="clear-data-store")
    @click.pass_context
    def clean_data_store(ctx):

        kiara_obj: Kiara = ctx.obj["kiara"]

        path = kiara_obj.data_store._base_path
        print()
        print(f"Deleting folder: {path}...")
        shutil.rmtree(path=path, ignore_errors=True)
        print("Folder deleted.")
