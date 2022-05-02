# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Data-related sub-commands for the cli."""
import rich_click as click
import shutil
import sys

from kiara import Kiara
from kiara.models.values.info import RENDER_FIELDS, ValueInfo, ValuesInfo
from kiara.utils import StringYAML, is_debug, is_develop, log_message
from kiara.utils.cli import output_format_option, terminal_print, terminal_print_model

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
    default=False,
)
@click.option(
    "--include-internal",
    "-I",
    help="Also list values that are used mostly internally (e.g. metadata for other values, ...). Implies 'all-ids' is 'True'.",
    is_flag=True,
)
@click.option(
    "--value_id",
    "-i",
    help="Display value id information for each value.",
    default=False,
    is_flag=True,
)
@click.option(
    "--type-config",
    "-c",
    help="Display type details for each value.",
    default=False,
    is_flag=True,
)
@click.option(
    "--pedigree",
    "-P",
    help="Display pedigree information for each value.",
    default=False,
    is_flag=True,
)
@click.option(
    "--data",
    "-d",
    help="Show a preview of the data associated with this value.",
    default=False,
    is_flag=True,
)
@click.option(
    "--serialized",
    "-s",
    help="Display serialization details for this value.",
    is_flag=True,
)
@click.option("--properties", "-p", help="Display the value properties.", is_flag=True)
@output_format_option()
@click.pass_context
def list_values(
    ctx,
    format,
    all_ids,
    include_internal,
    value_id,
    pedigree,
    data,
    type_config,
    serialized,
    properties,
):
    """List all data items that are stored in kiara."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    if include_internal:
        all_ids = True

    if not all_ids:
        alias_registry = kiara_obj.alias_registry
        value_ids = [v.value_id for v in alias_registry.aliases.values()]
    else:
        data_registry = kiara_obj.data_registry
        data_registry.retrieve_all_available_value_ids()
        value_ids = kiara_obj.data_registry.retrieve_all_available_value_ids()

    list_by_alias = True

    render_fields = [k for k, v in RENDER_FIELDS.items() if v["show_default"]]
    if list_by_alias:
        render_fields[0] = "aliases"
        render_fields[1] = "value_id"

    if not value_id and not all_ids:
        render_fields.remove("value_id")
    if type_config:
        render_fields.append("data_type_config")
    if data:
        render_fields.append("data")
    if properties:
        render_fields.append("properties")
    if pedigree:
        render_fields.append("pedigree")
    if serialized:
        render_fields.append("serialize_details")

    values_info_model = ValuesInfo.create_from_values(kiara_obj, *value_ids)

    render_config = {
        "render_type": "terminal",
        "list_by_alias": list_by_alias,
        "show_internal": include_internal,
        "render_fields": render_fields,
    }

    if not all_ids:
        title = "Available aliases"
    else:
        title = "Available values"

    terminal_print_model(
        values_info_model, format=format, in_panel=title, **render_config
    )


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
    "--pedigree", "-P", help="Display pedigree information for the value.", is_flag=True
)
@click.option(
    "--serialized",
    "-s",
    help="Display this values' serialization information.",
    is_flag=True,
)
@click.option("--preview-data", "-d", help="Display a data preview.", is_flag=True)
@click.option(
    "--properties",
    "-p",
    help="Resolve and display properties of this value.",
    is_flag=True,
)
@click.option(
    "--destinies",
    "-D",
    help="Resolve and display values destinies for this value.",
    is_flag=True,
)
@click.option(
    "--destiny-backlinks",
    "-B",
    help="Resolve and display values this value is a destiny for.",
    is_flag=True,
)
@output_format_option()
@click.pass_context
def explain_value(
    ctx,
    value_id: str,
    metadata: bool,
    pedigree: bool,
    serialized: bool,
    format: str,
    preview_data: bool,
    properties: bool,
    destinies: bool,
    destiny_backlinks: bool,
):
    """Print the metadata of a stored value.

    All of the 'show-additional-information' flags are only applied when the 'terminal' output format is selected. This might change in the future.
    """

    kiara_obj: Kiara = ctx.obj["kiara"]

    render_config = {
        "show_metadata": metadata,
        "show_pedigree": pedigree,
        "show_serialized": serialized,
        "show_data_preview": preview_data,
        "show_properties": properties,
        "show_destinies": destinies,
        "show_destiny_backlinks": destiny_backlinks,
    }

    all_values = []
    for v_id in value_id:
        value = kiara_obj.data_registry.get_value(v_id)
        if not value:
            terminal_print(f"No saved value found for: {v_id}")
            sys.exit(1)
        all_values.append(value)

    if len(all_values) == 1:
        title = f"Value details for: [b i]{v_id}[/b i]"
    else:
        title = "Value details"

    v_infos = (
        ValueInfo.create_from_value(kiara=kiara_obj, value=v) for v in all_values
    )

    terminal_print_model(*v_infos, format=format, in_panel=title, **render_config)


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
    # if value is None:
    #     print(f"No value available for id: {value_id}")
    #     sys.exit(1)

    try:
        renderable = kiara_obj.data_registry.render_data(
            value.value_id, target_type="terminal_renderable"
        )
    except Exception as e:

        if is_debug():
            import traceback

            traceback.print_exc()
        log_message("error.render_value", value=value.value_id, error=e)

        renderable = [str(value.data)]

    terminal_print(renderable)


if is_develop():

    @data.command(name="clear-data-store")
    @click.pass_context
    def clean_data_store(ctx):

        kiara_obj: Kiara = ctx.obj["kiara"]

        paths = {}

        data_store_path = kiara_obj.data_registry.get_archive().data_store_path
        paths["data_store"] = data_store_path

        aliases_store_path = kiara_obj.alias_registry.get_archive().alias_store_path
        paths["alias_store"] = aliases_store_path

        job_record_store_path = kiara_obj.job_registry.get_archive().job_store_path
        paths["jobs_record_store"] = job_record_store_path

        destiny_store_path = (
            kiara_obj.destiny_registry.default_destiny_store.destiny_store_path
        )
        paths["destiny_store"] = destiny_store_path

        print()
        for k, v in paths.items():
            print(f"Deleting {k}: {v}...")
            shutil.rmtree(path=v, ignore_errors=True)
            print("   -> done")
