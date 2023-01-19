# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""The 'run' subcommand for the cli."""

import os.path
import rich_click as click
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping, Union

from kiara import KiaraAPI
from kiara.utils.cli import dict_from_cli_args
from kiara.utils.cli.exceptions import handle_exception
from kiara.utils.cli.run import (
    _validate_save_option,
    calculate_aliases,
    execute_job,
    set_and_validate_inputs,
    validate_operation_in_terminal,
)
from kiara.utils.output import OutputDetails


@click.command()
@click.argument("module_or_operation", nargs=1, metavar="MODULE_OR_OPERATION")
@click.argument("inputs", nargs=-1, required=False)
@click.option(
    "--module-config",
    "-c",
    required=False,
    help="(Optional) module configuration, only valid when run target is a module name.",
    multiple=True,
)
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
@handle_exception()
def run(
    ctx,
    module_or_operation: str,
    module_config: Iterable[str],
    inputs: Iterable[str],
    output: Iterable[str],
    explain: bool,
    save: Iterable[str],
    help: bool,
):
    """Run a kiara operation."""

    # =========================================================================
    # initialize a few variables

    if module_config:
        module_config = dict_from_cli_args(*module_config)
    else:
        module_config = {}

    save_results = _validate_save_option(save)

    output_details = OutputDetails.from_data(output)
    silent = False
    if output_details.format == "silent":
        silent = True

    force_overwrite = output_details.config.get("force", False)

    if output_details.target != "terminal":
        if output_details.target == "file":
            target_dir = Path.cwd()
            target_file = target_dir / f"{module_or_operation}.{output_details.format}"
        else:
            target_file = Path(
                os.path.realpath(os.path.expanduser(output_details.target))
            )

        if target_file.exists() and not force_overwrite:
            print()
            print(
                f"Can't run workflow, the target files already exist, and '--output force=true' not specified: {target_file}"
            )
            sys.exit(1)

    # kiara_obj: Kiara = ctx.obj["kiara"]
    api: KiaraAPI = ctx.obj["kiara_api"]

    cmd_arg = ctx.params["module_or_operation"]
    cmd_help = f"[yellow bold]Usage: [/yellow bold][bold]kiara run [OPTIONS] [i]{cmd_arg}[/i] [INPUTS][/bold]"

    if module_config:
        op: Union[str, Mapping[str, Any]] = {
            "module_type": module_or_operation,
            "module_config": module_config,
        }
    else:
        op = module_or_operation

    kiara_op = validate_operation_in_terminal(
        api=api, module_or_operation=op, allow_external=True
    )
    final_aliases = calculate_aliases(operation=kiara_op, alias_tokens=save)

    inputs_value_map = set_and_validate_inputs(
        api=api,
        operation=kiara_op,
        inputs=inputs,
        explain=explain,
        print_help=help,
        click_context=ctx,
        cmd_help=cmd_help,
    )
    execute_job(
        api=api,
        operation=kiara_op,
        inputs=inputs_value_map,
        silent=silent,
        save_results=save_results,
        aliases=final_aliases,
    )
