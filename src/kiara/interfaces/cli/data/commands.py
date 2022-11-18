# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Data-related sub-commands for the cli."""
import rich_click as click
import structlog
import sys
from typing import Iterable, Tuple, Union

from kiara.context import Kiara
from kiara.interfaces.python_api import KiaraAPI
from kiara.interfaces.python_api.models.info import RENDER_FIELDS, ValueInfo, ValuesInfo
from kiara.interfaces.tui.pager import PagerApp
from kiara.models.module.operation import Operation
from kiara.operations.included_core_operations.filter import FilterOperationType
from kiara.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)
from kiara.utils import log_exception, log_message
from kiara.utils.cli import output_format_option, terminal_print, terminal_print_model
from kiara.utils.cli.run import (
    _validate_save_option,
    calculate_aliases,
    execute_job,
    set_and_validate_inputs,
    validate_operation_in_terminal,
)
from kiara.utils.output import OutputDetails
from kiara.utils.yaml import StringYAML

logger = structlog.getLogger()

yaml = StringYAML()


@click.group()
@click.pass_context
def data(ctx):
    """Data-related sub-commands."""


@data.command(name="list")
@click.option(
    "--all-values",
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
    "--hash", "-H", help="Display the value hash.", default=False, is_flag=True
)
@click.option(
    "--lineage",
    "-l",
    help="Display lineage information for each value.",
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
@click.option(
    "--data-type",
    "-t",
    help="Only display values that match the specified type(s)",
    multiple=True,
    required=False,
)
@output_format_option()
@click.pass_context
def list_values(
    ctx,
    format,
    all_values,
    hash,
    include_internal,
    value_id,
    pedigree,
    data,
    type_config,
    serialized,
    properties,
    data_type,
    lineage,
) -> None:
    """List all data items that are stored in kiara."""

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]

    if include_internal:
        all_values = True

    matcher_config = {"allow_internal": include_internal, "has_alias": not all_values}
    if data_type:
        matcher_config["data_types"] = data_type

    values = kiara_api.list_values(
        allow_internal=include_internal, data_types=data_type, has_alias=not all_values
    )

    list_by_alias = True

    render_fields = [k for k, v in RENDER_FIELDS.items() if v["show_default"]]
    if list_by_alias:
        render_fields[0] = "aliases"
        render_fields[1] = "value_id"

    if not value_id and not all_values:
        render_fields.remove("value_id")
    if type_config:
        render_fields.append("data_type_config")
    if hash:
        render_fields.append("hash")
    if data:
        render_fields.append("data")
    if properties:
        render_fields.append("properties")
    if pedigree:
        render_fields.append("pedigree")
    if lineage:
        render_fields.append("lineage")
    if serialized:
        render_fields.append("serialize_details")

    values_info_model = ValuesInfo.create_from_instances(
        kiara=kiara_api.context, instances={str(k): v for k, v in values.items()}
    )

    render_config = {
        "render_type": "terminal",
        "list_by_alias": list_by_alias,
        "show_internal_values": include_internal,
        "render_fields": render_fields,
    }

    if not all_values:
        title = "Available aliases"
    else:
        title = "Available values"

    terminal_print_model(
        values_info_model, format=format, in_panel=title, **render_config
    )


@data.command(name="explain")
@click.argument("value_id", nargs=-1, required=True)
@click.option(
    "--pedigree", "-P", help="Display pedigree information for the value.", is_flag=True
)
@click.option(
    "--lineage", "-l", help="Display lineage information for the value.", is_flag=True
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
@click.option(
    "--environment", "-e", help="Show environment hashes and data.", is_flag=True
)
@output_format_option()
@click.pass_context
def explain_value(
    ctx,
    value_id: Tuple[str],
    pedigree: bool,
    serialized: bool,
    format: str,
    preview_data: bool,
    properties: bool,
    destinies: bool,
    destiny_backlinks: bool,
    lineage: bool,
    environment: bool,
):
    """Print the metadata of a stored value.

    All of the 'show-additional-information' flags are only applied when the 'terminal' output format is selected. This might change in the future.
    """

    kiara_obj: Kiara = ctx.obj["kiara"]

    render_config = {
        "show_pedigree": pedigree,
        "show_serialized": serialized,
        "show_data_preview": preview_data,
        "show_properties": properties,
        "show_destinies": destinies,
        "show_destiny_backlinks": destiny_backlinks,
        "show_lineage": lineage,
        "show_environment_hashes": environment,
        "show_environment_data": False,
    }

    all_values = []
    for v_id in value_id:
        try:
            value = kiara_obj.data_registry.get_value(v_id)
        except Exception as e:
            terminal_print()
            terminal_print(f"[red]Error[/red]: {e}")
            sys.exit(1)
        if not value:
            terminal_print(f"[red]Error[/red]: No value found for: {v_id}")
            sys.exit(1)
        all_values.append(value)

    if len(all_values) == 1:
        title = f"Value details for: [b i]{value_id[0]}[/b i]"
    else:
        title = "Value details"

    v_infos = (
        ValueInfo.create_from_instance(kiara=kiara_obj, instance=v) for v in all_values
    )

    terminal_print_model(*v_infos, format=format, in_panel=title, **render_config)


@data.command(name="load")
@click.argument("value", nargs=1, required=True)
@click.option(
    "--single-page",
    "-s",
    help="Only pretty print a single (preview) page, instead of using a pager when available.",
    is_flag=True,
)
@click.pass_context
def load_value(ctx, value: str, single_page: bool):
    """Load a stored value and print it in a format suitable for the terminal."""

    # kiara_obj: Kiara = ctx.obj["kiara"]
    kiara_api: KiaraAPI = ctx.obj["kiara_api"]

    try:
        _value = kiara_api.get_value(value=value)
    except Exception as e:
        terminal_print()
        terminal_print(f"[red]Error[/red]: {e}")
        sys.exit(1)
    if not _value:
        terminal_print(f"[red]Error[/red]: No value found for: {value}")
        sys.exit(1)

    render_op: Union[Operation, None] = None
    if not single_page:
        render_value_op_type: RenderValueOperationType = kiara_api.get_operation_type(
            op_type=RenderValueOperationType
        )
        render_op = render_value_op_type.get_render_operation(
            source_type=_value.data_type_name, target_type="terminal_renderable"
        )
    else:
        logger.debug(
            "fallback.render_value",
            solution="use pretty print",
            source_type=_value.data_type_name,
            target_type="terminal_renderable",
            reason="no 'render_value' operation for source/target operation",
        )
        try:
            renderable = kiara_api.context.data_registry.pretty_print_data(
                _value.value_id, target_type="terminal_renderable"
            )
        except Exception as e:
            log_exception(e)
            log_message("error.pretty_print", value=_value.value_id, error=e)
            renderable = [str(_value.data)]

        terminal_print(renderable)
        sys.exit(0)

    # log_file = None
    # if is_develop() or is_debug() or True:
    #     log_file = "kiara_pager.log"
    PagerApp.run(kiara_api=kiara_api, value=_value, operation=render_op)

    # inputs = {
    #     "value": _value,
    #     "render_scene": {}
    # }
    # result = render_op.run(kiara=kiara_api.context, inputs=inputs)
    # dbg(result["render_scene_result"].data)


@data.command("filter")
@click.argument("value", nargs=1, required=True)
@click.argument("filters", nargs=1)
@click.argument("inputs", nargs=-1, required=False)
@click.option(
    "--explain",
    "-e",
    help="Display information about the selected operation and exit.",
    is_flag=True,
)
@click.option(
    "--output", "-o", help="The output format and configuration.", multiple=True
)
@click.option(
    "--save",
    "-s",
    help="Save one or several of the outputs of this run. If the argument contains a '=', the format is [output_name]=[alias], if not, the values will be saved as '[alias]-[output_name]'.",
    required=False,
    multiple=True,
)
@click.option("--help", "-h", help="Show this message and exit.", is_flag=True)
@click.pass_context
def filter_value(
    ctx,
    value: str,
    filters: str,
    inputs: Iterable[str],
    explain: bool,
    output: Iterable[str],
    save: Iterable[str],
    help: bool,
):
    """Load a value, fiter it, then display."""

    save_results = _validate_save_option(save)

    output_details = OutputDetails.from_data(output)
    silent = False
    if output_details.format == "silent":
        silent = True

    kiara_obj: Kiara = ctx.obj["kiara"]

    try:
        _value = kiara_obj.data_registry.get_value(value=value)
    except Exception as e:
        terminal_print()
        terminal_print(f"[red]Error[/red]: {e}")
        sys.exit(1)
    if not _value:
        terminal_print(f"[red]Error[/red]: No value found for: {value}")
        sys.exit(1)

    filter_op_type: FilterOperationType = kiara_obj.operation_registry.get_operation_type("filter")  # type: ignore

    _filter_names = filters.split(",")
    filter_names = []
    for fn in _filter_names:
        filter_names.extend(fn.split(":"))
    op = filter_op_type.create_filter_operation(
        data_type=_value.data_type_name, filters=filter_names
    )

    all_inputs = [f"value={value}"]
    all_inputs.extend(inputs)

    cmd_help = "[yellow bold]Usage: [/yellow bold][bold]kiara data filter VALUE FILTER_1:FILTER_2 [FILTER ARGS...][/bold]"

    kiara_op = validate_operation_in_terminal(
        kiara=kiara_obj, module_or_operation="pipeline", module_config=op.module_config
    )
    final_aliases = calculate_aliases(kiara_op=kiara_op, alias_tokens=save)
    set_and_validate_inputs(
        kiara_op=kiara_op,
        inputs=all_inputs,
        explain=explain,
        print_help=help,
        click_context=ctx,
        cmd_help=cmd_help,
    )
    execute_job(
        kiara_op=kiara_op,
        silent=silent,
        save_results=save_results,
        aliases=final_aliases,
    )
