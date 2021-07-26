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

    # for alias, details in kiara_obj.data_store.aliases.items():
    #     print("-----")
    #     print(alias)
    #     import pp
    #     pp(details)
    #     only_latest = kiara_obj.data_store.get_alias_versions(alias)
    #     for version, d in only_latest.items():
    #         print('---')
    #         print(version)
    #         pp(d)

    # kiara_obj.data_store
    # aliases = kiara_obj.data_store.get_aliases_for_id(v_id)
    #     aliases = []
    #
    # rich_print(table)

    # else:
    #     for id_or_alias, v_id in kiara_obj.data_store.ids_or_aliases.items():
    #         v_type = kiara_obj.data_store.get_value_type(v_id)
    #         if not details:
    #             rich_print(f"  - [b]{id_or_alias}[/b]: {v_type}")
    #         else:
    #             rich_print(f"[b]{id_or_alias}[/b]: {v_type}\n")
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
