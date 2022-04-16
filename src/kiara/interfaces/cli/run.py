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
from pydantic import ValidationError
from rich.console import Group
from typing import Dict, Iterable, List

from kiara import Kiara
from kiara.exceptions import (
    FailedJobException,
    InvalidValuesException,
    NoSuchExecutionTargetException,
)
from kiara.interfaces.python_api import KiaraOperation
from kiara.utils import dict_from_cli_args, is_debug
from kiara.utils.cli import terminal_print
from kiara.utils.output import OutputDetails, create_table_from_base_model_cls


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
    help="Display additional runtime information.",
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
@click.pass_context
def run(
    ctx,
    module_or_operation: str,
    module_config: Iterable[str],
    inputs: Iterable[str],
    output: Iterable[str],
    explain: bool,
    save: Iterable[str],
):

    # =========================================================================
    # initialize a few variables
    if module_config:
        module_config = dict_from_cli_args(*module_config)
    else:
        module_config = {}

    if not save:
        aliases: Dict[str, List[str]] = {}
        full_aliases: List[str] = []
    else:
        aliases = {}
        full_aliases = []
        for a in save:
            if "=" not in a:
                full_aliases.append(a)
            else:
                tokens = a.split("=")
                if len(tokens) != 2:
                    print()
                    print(f"Invalid alias format, can only contain a single '=': {a}")
                    sys.exit(1)

                aliases.setdefault(tokens[0], []).append(tokens[1])

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

    kiara_obj: Kiara = ctx.obj["kiara"]

    kiara_op = KiaraOperation(
        kiara=kiara_obj,
        operation_name=module_or_operation,
        operation_config=module_config,
    )
    try:
        # validate that operation config is valid, ignoring inputs for now
        operation = kiara_op.operation
    except NoSuchExecutionTargetException as nset:
        print()
        terminal_print(nset)
        print()
        print("Existing operations:")
        print()
        for n in nset.avaliable_targets:
            terminal_print(f"  - [i]{n}[/i]")
        sys.exit(1)
    except ValidationError as ve:

        renderables = [""]
        renderables.append("Invalid module configuration:")
        renderables.append("")
        for error in ve.errors():
            loc = ", ".join(error["loc"])  # type: ignore
            renderables.append(f"  [b]{loc}[/b]: [red]{error['msg']}[/red]")

        try:
            m = kiara_obj.module_registry.get_module_class(kiara_op.operation_name)
            schema = create_table_from_base_model_cls(m._config_cls)
            renderables.append("")
            renderables.append(f"Module configuration schema for '[b i]{m._module_type_name}[/b i]':")  # type: ignore
            renderables.append("")
            renderables.append(schema)
        except Exception:
            pass

        msg = Group(*renderables)
        terminal_print()
        terminal_print(msg, in_panel="[b red]Module configuration error[/b red]")
        sys.exit(1)
    except Exception as e:
        terminal_print()
        if is_debug():
            import traceback

            traceback.print_exc()
        terminal_print(str(e))
        sys.exit(1)

    # =========================================================================
    # check save user input
    final_aliases = {}
    if save:
        op_output_names = operation.outputs_schema.keys()
        invalid_fields = []
        for field_name, alias in aliases.items():
            if field_name not in op_output_names:
                invalid_fields.append(field_name)
            else:
                final_aliases[field_name] = alias

        for _alias in full_aliases:
            for field_name in op_output_names:
                final_aliases.setdefault(field_name, []).append(
                    f"{_alias}.{field_name}"
                )

        if invalid_fields:
            print()
            print(
                f"Can't run workflow, invalid field name(s) when specifying aliases: {', '.join(invalid_fields)}. Valid field names: {', '.join(op_output_names)}"
            )
            sys.exit(1)

    # =========================================================================
    # prepare inputs
    list_keys = []
    for name, value_schema in operation.operation_details.inputs_schema.items():
        if value_schema.type in ["array", "list"]:
            list_keys.append(name)

    inputs_dict = dict_from_cli_args(*inputs, list_keys=list_keys)

    kiara_op.set_inputs(**inputs_dict)

    try:
        operation_inputs = kiara_op.operation_inputs
    except InvalidValuesException as ive:
        print()
        terminal_print(str(ive))
        print()
        print("Details:\n")
        for k, v in ive.invalid_inputs.items():
            terminal_print(f"  - [b]{k}[/b]: [i]{v}[/i]")
        sys.exit(1)

    invalid = operation_inputs.check_invalid()
    if invalid:
        print()
        print("Can't create operation inputs, invalid field(s):")
        print()
        for k, v in invalid.items():
            terminal_print(f"  - [b]{k}[/b]: [i]{v}[/i]")
        sys.exit(1)

    # =========================================================================
    # execute job
    job_id = kiara_op.queue_job()

    try:
        outputs = kiara_op.retrieve_result(job_id=job_id)
    except FailedJobException as fje:
        print()
        terminal_print(f"[red b]Job failed[/red b]: {fje.job.error}")
        sys.exit(1)

    if not silent:
        print()
        if len(outputs) > 1:
            terminal_print("[b]Results:[/b]")
        else:
            terminal_print("[b]Result:[/b]")

        for field_name, value in outputs.items():
            print()
            terminal_print(f"* [b i]{field_name}[/b i]")
            terminal_print(kiara_obj.data_registry.render_data(value.value_id))

    # for k, v in outputs.items():
    #     rendered = kiara_obj.data_registry.render_data(v)
    #     rich_print(rendered)

    if save:

        save_results = kiara_op.save_result(job_id=job_id, aliases=final_aliases)
        if not silent:
            print()
            terminal_print("[b]Stored value(s):[/b]")
            terminal_print(save_results)
