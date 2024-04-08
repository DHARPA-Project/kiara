# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Data-related sub-commands for the cli."""
import os.path
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Tuple, Union

import rich_click as click
import structlog

from kiara.defaults import DATA_ARCHIVE_DEFAULT_VALUE_MARKER
from kiara.exceptions import InvalidCommandLineInvocation
from kiara.utils import is_develop, log_exception, log_message
from kiara.utils.cli import output_format_option, terminal_print, terminal_print_model
from kiara.utils.cli.exceptions import handle_exception

# from kiara.interfaces.python_api.models.info import RENDER_FIELDS, ValueInfo, ValuesInfo
# from kiara.interfaces.tui.pager import PagerApp
# from kiara.operations.included_core_operations.filter import FilterOperationType


# from kiara.utils.cli.rich_click import rich_format_filter_operation_help
# from kiara.utils.cli.run import (
#     _validate_save_option,
#     calculate_aliases,
#     execute_job,
#     set_and_validate_inputs,
#     validate_operation_in_terminal,
# )
# from kiara.utils.output import OutputDetails


if TYPE_CHECKING:
    from kiara.interfaces.python_api.base_api import BaseAPI, Kiara
    from kiara.operations.included_core_operations.filter import FilterOperationType

logger = structlog.getLogger()


@click.group()
@click.pass_context
def data(ctx):
    """Data-related sub-commands."""


@data.command(name="list")
@click.argument("filter", nargs=-1, required=False)
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
    help="Also list values that are used mostly internally (e.g. metadata for other values, ...). Implies 'all-ids' "
    "is 'True'.",
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
    "--date-created",
    "-D",
    help="Display the date when the value was created.",
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
@handle_exception()
def list_values(
    ctx,
    filter,
    format,
    all_values,
    hash,
    include_internal,
    value_id,
    date_created,
    pedigree,
    data,
    type_config,
    serialized,
    properties,
    data_type,
    lineage,
) -> None:
    """List all data items that are stored in kiara."""

    from kiara.interfaces.python_api.models.info import RENDER_FIELDS, ValuesInfo

    kiara_api: BaseAPI = ctx.obj.base_api

    if include_internal:
        all_values = True

    matcher_config = {"allow_internal": include_internal, "has_alias": not all_values}
    if data_type:
        matcher_config["data_types"] = data_type

    if filter:
        matcher_config["alias_matchers"] = filter

    values = kiara_api.list_values(**matcher_config)

    list_by_alias = True

    render_fields = [k for k, v in RENDER_FIELDS.items() if v["show_default"]]
    if list_by_alias:
        render_fields[0] = "aliases"
        render_fields[1] = "value_id"

    if not value_id and not all_values:
        render_fields.remove("value_id")

    if date_created:
        render_fields.append("value_created")
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
@handle_exception()
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
    """
    Print the metadata of a stored value.

    All of the 'show-additional-information' flags are only applied when the 'terminal' output format is selected. This might change in the future.
    """

    kiara_api: BaseAPI = ctx.obj.base_api

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
            value_info = kiara_api.retrieve_value_info(v_id)
        except Exception as e:
            terminal_print()
            terminal_print(f"[red]Error[/red]: {e}")
            sys.exit(1)
        if not value_info:
            terminal_print(f"[red]Error[/red]: No value found for: {v_id}")
            sys.exit(1)
        all_values.append(value_info)

    if len(all_values) == 1:
        title = f"Value details for: [b i]{value_id[0]}[/b i]"
    else:
        title = "Value details"

    # v_infos = (
    #     ValueInfo.create_from_instance(kiara=kiara_obj, instance=v) for v in all_values
    # )

    terminal_print_model(*all_values, format=format, in_panel=title, **render_config)


@data.command(name="load")
@click.argument("value", nargs=1, required=True)
# @click.option(
#     "--single-page",
#     "-s",
#     help="Only pretty print a single (preview) page, instead of using a pager when available.",
#     is_flag=True,
# )
@click.pass_context
@handle_exception()
def load_value(ctx, value: str):
    """Load a stored value and print it in a format suitable for the terminal."""
    # kiara_obj: Kiara = ctx.obj["kiara"]
    kiara_api: BaseAPI = ctx.obj.base_api

    try:
        _value = kiara_api.get_value(value=value)
    except Exception as e:
        terminal_print()
        terminal_print(f"[red]Error[/red]: {e}")
        sys.exit(1)
    if not _value:
        terminal_print(f"[red]Error[/red]: No value found for: {value}")
        sys.exit(1)

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
    # from kiara.interfaces.tui.pager import PagerApp
    #
    # app = PagerApp(api=kiara_api, value=str(_value.value_id))
    # app.run()


@data.command("filter")
@click.argument("value", nargs=1, required=True, default="__no_value__")
@click.argument("filters", nargs=1, default="__no_filters__")
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
    """
    Filter a value, then display it like the 'load' subcommand does.

    Filters must be provided as a single string, where filters are seperated using ":".
    """
    from kiara.utils.cli.rich_click import rich_format_filter_operation_help
    from kiara.utils.cli.run import (
        _validate_save_option,
        calculate_aliases,
        execute_job,
        set_and_validate_inputs,
        validate_operation_in_terminal,
    )
    from kiara.utils.output import OutputDetails

    save_results = _validate_save_option(save)

    output_details = OutputDetails.from_data(output)
    silent = False
    if output_details.format == "silent":
        silent = True

    kiara_obj: Kiara = ctx.obj.kiara
    api: BaseAPI = ctx.obj.base_api

    cmd_help = "[yellow bold]Usage: [/yellow bold][bold]kiara data filter VALUE FILTER_1:FILTER_2 [FILTER ARGS...][/bold]"

    if help and value == "__no_value__":
        rich_format_filter_operation_help(
            api=api,
            obj=ctx.command,
            ctx=ctx,
            cmd_help=cmd_help,
        )
        sys.exit(0)

    if filters == "__no_filters__":
        rich_format_filter_operation_help(
            api=api, obj=ctx.command, ctx=ctx, cmd_help=cmd_help, value=value
        )
        sys.exit(0)

    try:
        _value = kiara_obj.data_registry.get_value(value=value)
    except Exception as e:
        terminal_print()
        terminal_print(f"[red]Error[/red]: {e}")
        sys.exit(1)
    if not _value:
        terminal_print(f"[red]Error[/red]: No value found for: {value}")
        sys.exit(1)

    _filter_names = filters.split(":")
    filter_names = []
    for fn in _filter_names:
        filter_names.extend(fn.split(":"))

    filter_op_type: FilterOperationType = (
        kiara_obj.operation_registry.get_operation_type("filter")
    )
    op = filter_op_type.create_filter_operation(
        data_type=_value.data_type_name, filters=filter_names
    )

    all_inputs = [f"value={value}"]
    all_inputs.extend(inputs)

    try:
        kiara_op = validate_operation_in_terminal(
            api=api, module_or_operation=op.module_config
        )
    except InvalidCommandLineInvocation as e:
        ctx.obj.exit(msg=None, exit_code=e.error_code)
        sys.exit(1)

    final_aliases = calculate_aliases(operation=kiara_op, alias_tokens=save)
    try:
        inputs_value_map = set_and_validate_inputs(
            api=api,
            operation=kiara_op,
            inputs=all_inputs,
            explain=explain,
            print_help=help,
            click_context=ctx,
            cmd_help=cmd_help,
        )
        if inputs_value_map is None:
            ctx.obj.exit(msg=None, exit_code=0)
            return
    except InvalidCommandLineInvocation as e:
        ctx.obj.exit(msg=None, exit_code=e.error_code)
        return

    job_id = execute_job(
        api=api,
        operation=kiara_op,
        inputs=inputs_value_map,
        silent=True,
        save_results=False,
        aliases=final_aliases,
    )

    if not silent:
        result = api.get_job_result(job_id=job_id)
        # result = kiara_op.retrieve_result(job_id=job_id)

        title = f"[b]Value '[i]{value}[/i]'[/b], filtered with: {filters}"
        filtered = result["filtered_value"]
        try:
            renderable = api.context.data_registry.pretty_print_data(
                filtered.value_id, target_type="terminal_renderable"
            )
        except Exception as e:
            log_exception(e)
            log_message("error.pretty_print", value=_value.value_id, error=e)
            renderable = [str(_value.data)]
        terminal_print(
            renderable, in_panel=title, empty_line_before=True, show_data_type=True
        )

    if save_results:
        try:
            result = api.get_job_result(job_id=job_id)
            saved_results = api.store_values(result, alias_map=final_aliases)

            api.context.job_registry.store_job_record(job_id=job_id)

            if len(saved_results) == 1:
                title = "[b]Stored result value[/b]"
            else:
                title = "[b]Stored result values[/b]"
            terminal_print(saved_results, in_panel=title, empty_line_before=True)
        except Exception as e:
            log_exception(e)
            terminal_print(f"[red]Error saving results[/red]: {e}")
            sys.exit(1)


@data.command(name="export")
@click.option(
    "--archive-name",
    "-A",
    help="The name to use for the exported archive. If not provided, the first alias will be used.",
    required=False,
)
@click.option(
    "--path",
    "-p",
    help="The path of the exported archive. If not provided, '<archive_alias>.karchive' will be used.",
    required=False,
)
@click.option(
    "--compression",
    "-c",
    help="The compression inside the archive. If not provided, 'zstd' will be used.",
    type=click.Choice(["zstd", "lz4", "lzma", "none"]),
    default="zstd",
)
@click.option("--append", "-a", help="Append data to existing archive.", is_flag=True)
@click.option("--replace", help="Replace existing archive.", is_flag=True)
# @click.option(
#     "--no-default-value", "-nd", help="Do not set a default value.", is_flag=True
# )
# @click.option("--no-aliases", "-na", help="Do not store aliases.", is_flag=True)
@click.argument("aliases", nargs=-1, required=True)
@click.pass_context
@handle_exception()
def export_data_archive(
    ctx,
    aliases: Tuple[str],
    archive_name: Union[None, str],
    path: Union[str, None],
    compression: str,
    append: bool,
    replace: bool,
    no_default_value: bool = False,
    no_aliases: bool = False,
):
    """Export one or several values into a new (or existing) kiara archive.

    Aliases that already exist in the target archve will be overwritten.
    """

    kiara_api: BaseAPI = ctx.obj.base_api

    values = []
    for idx, alias in enumerate(aliases, start=1):
        if "=" in alias:
            old_alias, new_alias = alias.split("=", maxsplit=1)
        else:
            try:
                uuid.UUID(alias)
                old_alias = alias
                new_alias = None
            except Exception:
                old_alias = alias
                new_alias = alias

        value = kiara_api.get_value(old_alias)
        values.append((value, new_alias))

    if not archive_name:
        archive_name = values[0][1]

    if not archive_name:
        archive_name = str(values[0][0].value_id)

    if not path:
        base_path = "."
        if archive_name.endswith(".kiarchive"):
            file_name = archive_name
        else:
            file_name = f"{archive_name}.kiarchive"
    else:
        base_path = os.path.dirname(path)
        file_name = os.path.basename(path)
        if "." not in file_name:
            file_name = f"{file_name}.kiarchive"

    full_path = Path(base_path) / file_name

    delete = False

    if full_path.exists() and (not append and not replace):
        terminal_print(
            f"[red]Error[/red]: File '{full_path}' already exists and '--append' or '--replace' not specified."
        )
        sys.exit(1)
    elif full_path.exists():
        if append and replace:
            terminal_print(
                "[red]Error[/red]: Can't specify both '--append' and '--replace'."
            )
            sys.exit(1)
        if append:
            terminal_print(f"Appending to existing data_store '{file_name}'...")
        else:
            terminal_print(f"Replacing existing data_store '{file_name}'...")
            delete = True
    else:
        terminal_print(f"Creating new data_store '{file_name}'...")

    terminal_print("Exporting value(s) into new data_store...")

    # no_default_value = False
    #
    # if not no_default_value:
    #     try:
    #         data_store.set_archive_metadata_value(
    #             DATA_ARCHIVE_DEFAULT_VALUE_MARKER, str(values[0][0].value_id)
    #         )
    #     except Exception as e:
    #         data_store.delete_archive(archive_id=data_store.archive_id)
    #         log_exception(e)
    #         terminal_print(f"[red]Error setting value[/red]: {e}")
    #         sys.exit(1)

    values_to_store = {}
    alias_map = {}
    for idx, (value, value_alias) in enumerate(values, start=1):
        key = f"value_{idx}"
        values_to_store[key] = value
        if value_alias:
            alias_map[key] = [value_alias]

    target_store_params = {
        "compression": compression,
    }
    try:
        no_default_value = False
        if not no_default_value:
            metadata_to_add = {
                DATA_ARCHIVE_DEFAULT_VALUE_MARKER: str(values[0][0].value_id)
            }
        else:
            metadata_to_add = None

        if delete:
            os.unlink(full_path)

        store_result = kiara_api.export_values(
            target_archive=full_path,
            values=values_to_store,
            alias_map=alias_map,
            allow_alias_overwrite=True,
            target_registered_name=archive_name,
            append=append,
            target_store_params=target_store_params,
            additional_archive_metadata=metadata_to_add,
        )
        render_config = {"add_field_column": False}
        terminal_print_model(
            store_result,
            format="terminal",
            empty_line_before=None,
            in_panel="Exported values",
            **render_config,
        )

    except Exception as e:
        # TODO: remove archive if it didn't exist before?
        log_exception(e)
        terminal_print(f"[red]Error saving results[/red]: {e}")
        sys.exit(1)


@data.command(name="import")
@click.argument("archive", nargs=1, required=True)
@click.argument("values", nargs=-1, required=True)
@click.option("--no-aliases", "-na", help="Do not store aliases.", is_flag=True)
@click.pass_context
@handle_exception()
def import_data_store(ctx, archive: str, values: Tuple[str], no_aliases: bool = False):
    """Import one or several values from a kiara archive."""

    kiara_api: BaseAPI = ctx.obj.base_api

    archive_path = Path(archive)
    if not archive_path.exists():
        terminal_print()
        terminal_print(f"[red]Error[/red]: Archive '{archive}' does not exist.")
        sys.exit(1)

    result = kiara_api.import_values(
        source_archive=archive, values=values, alias_map=not no_aliases
    )
    terminal_print(result)

    terminal_print("Done.")


if is_develop():

    @data.command(name="write_value")
    @click.argument("value_id_or_alias", nargs=1, required=True)
    @click.option(
        "--directory",
        "-d",
        help="The directory to write the serialized value to.",
        required=False,
    )
    @click.option(
        "--force", "-f", help="Overwrite existing files.", is_flag=True, default=False
    )
    @click.pass_context
    @handle_exception()
    def write_serialized(ctx, value_id_or_alias: str, directory: str, force: bool):
        """Write the serialized form of a value to a directory"""

        kiara_api: BaseAPI = ctx.obj.base_api

        value = kiara_api.get_value(value_id_or_alias)
        serialized = value.serialized_data

        keys = serialized.get_keys()

        if not directory:
            directory = "."

        path = Path(directory)

        for key in keys:
            data = serialized.get_serialized_data(key)

            key_path = path / key
            if key_path.exists() and not force:
                terminal_print(f"Error writing file for '{key}': file already exists.")
                sys.exit(1)

            key_path.parent.mkdir(parents=True, exist_ok=True)

            chunks = data.get_chunks(as_files=False)

            terminal_print(f"- writing file for: {key}")
            with open(key_path, "wb") as f:
                for chunk in chunks:
                    f.write(chunk)  # type: ignore
