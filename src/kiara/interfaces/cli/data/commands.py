# -*- coding: utf-8 -*-
"""Data-related sub-commands for the cli."""
import click
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

        value = kiara_obj.data_store.get_value_obj(v_id)
        value_type = value.type_name
        aliases = kiara_obj.data_store.find_aliases_for_value(
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
        print()
        value = kiara_obj.data_store.get_value_obj(value_item=v_id)
        if not value:
            print(f"No saved value found for: {v_id}")
            continue
        table = value.get_info().create_renderable(
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


@data.command(name="explain-lineage")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def explain_lineage(ctx, value_id: str):

    kiara_obj: Kiara = ctx.obj["kiara"]

    value = kiara_obj.data_store.get_value_obj(value_item=value_id)
    if value is None:
        print(f"No value stored for: {value_id}")
        sys.exit(1)

    value_info = value.create_info()

    lineage = value_info.lineage
    if not lineage:
        print(f"No lineage information associated to value '{value_id}'.")
        sys.exit(0)

    yaml_str = yaml.dump(lineage.to_minimal_dict())
    syntax = Syntax(yaml_str, "yaml", background_color="default")
    rich_print(
        Panel(
            syntax,
            title=f"Lineage for: {value_id}",
            title_align="left",
            box=box.ROUNDED,
            padding=(1, 0, 0, 2),
        )
    )


@data.command(name="load")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def load_value(ctx, value_id: str):
    """Load a stored value and print it in a format suitable for the terminal."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value = kiara_obj.data_store.get_value_obj(value_item=value_id)
    if value is None:
        print(f"No value available for id: {value_id}")
        sys.exit(1)

    try:
        renderables = kiara_obj.pretty_print(value, "renderables")
    except Exception as e:

        if is_debug():
            print(e)
        renderables = [str(value.get_value_data())]

    if len(renderables) == 0:
        return
    elif len(renderables) == 1:
        rich_print(renderables[0])
    else:
        rg = RenderGroup(*renderables)
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
