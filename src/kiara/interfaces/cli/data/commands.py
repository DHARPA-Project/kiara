# -*- coding: utf-8 -*-
"""Data-related sub-commands for the cli."""
import asyncclick as click
import shutil
from rich import box
from rich.table import Table

from kiara import Kiara
from kiara.utils import is_debug, is_develop
from kiara.utils.output import rich_print


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
@click.pass_context
def list_values(ctx, with_alias, only_latest, tags, all):
    """List all data items that are stored in kiara."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    table = Table(box=box.SIMPLE)

    if all:
        with_alias = False
        only_latest = False
        # tags = True

    table.add_column("id", style="i")
    table.add_column("aliases")
    table.add_column("type")

    for v_id in kiara_obj.data_store.value_ids:

        value_type = kiara_obj.data_store.get_value_type_for_id(v_id)
        aliases = kiara_obj.data_store.find_aliases_for_value_id(
            v_id, include_all_versions=not only_latest
        )

        if with_alias:
            if not aliases:
                continue

        _aliases = []
        if not aliases:
            _aliases.append("")
        else:
            for a in aliases:
                latest_alias = kiara_obj.data_store.get_latest_version_for_alias(
                    a.alias
                )
                if not only_latest:
                    if latest_alias == a.version:
                        _aliases.append(
                            f"[bold yellow2]{a.alias}[/bold yellow2]@{a.version}"
                        )
                    else:
                        _aliases.append(a.full_alias)
                else:
                    _aliases.append(a.alias)

        table.add_row(v_id, _aliases[0], value_type)

        for a in _aliases[1:]:
            table.add_row("", a, "")

    rich_print(table)


@data.command(name="explain")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def explain_value(ctx, value_id: str):
    """Print the metadata of a stored value."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value = kiara_obj.data_store.load_value(value_id=value_id)
    rich_print(value)


@data.command(name="load")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def load_value(ctx, value_id: str):
    """Load a stored value and print it in a format suitable for the terminal."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value = kiara_obj.data_store.load_value(value_id=value_id)

    try:
        renderables = kiara_obj.pretty_print(value, "renderables")
    except Exception as e:
        if is_debug():
            print(e)
        renderables = [str(value.get_value_data())]

    rich_print(*renderables)


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
