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
from rich.panel import Panel
from rich.table import Table

from kiara import Kiara
from kiara.utils import StringYAML, is_debug, is_develop, log_message, rich_print

yaml = StringYAML()


@click.group()
@click.pass_context
def data(ctx):
    """Data-related sub-commands."""


@data.command(name="list")
@click.option(
    "--all-ids",
    "-a",
    help="Also list values without aliases.",
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
    "--all-info",
    "-i",
    help="Display all information and values. Overrides the other options.",
    is_flag=True,
)
@click.option(
    "--show-pedigree",
    "-p",
    help="Display pedigree information for each value.",
    default=False,
    is_flag=True,
)
@click.option(
    "--show-data",
    "-d",
    help="Show a preview of the data associated with this value.",
    default=False,
    is_flag=True,
)
@click.option(
    "--show-load-config", "-l", help="Display this values' load config.", is_flag=True
)
@click.pass_context
def list_values(
    ctx,
    all_ids,
    only_latest,
    tags,
    all_info,
    show_pedigree,
    show_data,
    show_load_config,
):
    """List all data items that are stored in kiara."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    if all_ids:

        alias_registry = kiara_obj.alias_registry

        table = Table(show_lines=True, box=box.SIMPLE)
        table.add_column("alias")
        table.add_column("value")

        for alias in alias_registry.all_aliases:

            value_id = alias_registry.find_value_id_for_alias(alias)
            rendered = kiara_obj.data_registry.render_data(value_id=value_id)
            table.add_row(alias, rendered)

        rich_print(table)
    else:

        table = kiara_obj.data_registry.default_data_store.create_renderable(
            show_pedigree=show_pedigree,
            show_data=show_data,
            show_load_config=show_load_config,
        )
        rich_print(table, show_pedigree=show_pedigree, show_data=show_data)


@data.command(name="explain")
@click.argument("value_id", nargs=-1, required=True)
@click.option(
    "--metadata/--no-metadata",
    "-m",
    help="Display value metadata.",
    is_flag=True,
    default=True,
)
@click.option(
    "--pedigree", "-p", help="Display pedigree information for the value.", is_flag=True
)
@click.option(
    "--load-config", "-l", help="Display this values' load config.", is_flag=True
)
@click.pass_context
def explain_value(
    ctx, value_id: str, metadata: bool, pedigree: bool, load_config: bool
):
    """Print the metadata of a stored value."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    for v_id in value_id:
        value = kiara_obj.data_registry.get_value(v_id)
        print()
        if not value:
            print(f"No saved value found for: {v_id}")
            continue
        table = value.create_renderable(
            show_metadata=metadata, show_pedigree=pedigree, show_load_config=load_config
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

    value = kiara_obj.data_registry.get_value(value_id=value_id)
    if value is None:
        print(f"No value available for id: {value_id}")
        sys.exit(1)

    try:
        renderable = kiara_obj.data_registry.render_data(
            value_id, target_type="terminal_renderable"
        )
    except Exception as e:

        if is_debug():
            import traceback

            traceback.print_exc()
        log_message("error.render_value", value=value.value_id, error=e)

        renderable = [str(value.data)]

    rich_print(renderable)


if is_develop():

    @data.command(name="clear-data-store")
    @click.pass_context
    def clean_data_store(ctx):

        kiara_obj: Kiara = ctx.obj["kiara"]

        paths = {}

        data_store_path = kiara_obj.data_registry.default_data_store.data_store_path
        paths["data_store"] = data_store_path

        aliases_store_path = kiara_obj.alias_registry.default_alias_store.alias_store_path
        paths["alias_store"] = aliases_store_path

        job_record_store_path = kiara_obj.job_registry.default_job_store.job_store_path
        paths["jobs_record_store"] = job_record_store_path

        destiny_store_path = kiara_obj.destiny_registry.default_destiny_store.destiny_store_path
        paths["destiny_store"] = destiny_store_path

        print()
        for k, v in paths.items():
            print(f"Deleting {k}: {v}...")
            shutil.rmtree(path=v, ignore_errors=True)
            print("   -> done")
