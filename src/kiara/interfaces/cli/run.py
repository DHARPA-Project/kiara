# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""The 'run' subcommand for the cli."""

import os.path
import sys
import typing
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Union

import rich_click as click

from kiara.exceptions import InvalidCommandLineInvocation
from kiara.utils import log_message
from kiara.utils.cli import dict_from_cli_args, terminal_print
from kiara.utils.cli.exceptions import handle_exception
from kiara.utils.files import get_data_from_file

if typing.TYPE_CHECKING:
    from kiara.interfaces.python_api.base_api import BaseAPI


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
    "--comment", "-c", help="Add comment metadata to the job you run.", required=False
)
@click.option(
    "--save",
    "-s",
    help="Save one or several of the outputs of this run. If the argument contains a '=', the format is [output_name]=[alias], if not, the values will be saved as '[alias]-[output_name]'.",
    required=False,
    multiple=True,
)
@click.option(
    "--print-properties",
    "-p",
    help="Also display the properties of the result values.",
    required=False,
    is_flag=True,
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
    comment: Union[str, None],
    explain: bool,
    save: Iterable[str],
    print_properties: bool,
    help: bool,
):
    """Run a kiara operation."""
    from kiara.api import JobDesc, RunSpec
    from kiara.utils.cli.run import (
        _validate_save_option,
        calculate_aliases,
        execute_job,
        set_and_validate_inputs,
        validate_operation_in_terminal,
    )
    from kiara.utils.output import OutputDetails

    # =========================================================================
    # initialize a few variables

    if module_config:
        module_config = dict_from_cli_args(*module_config)
    else:
        module_config = {}

    _validate_save_option(save)

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
            terminal_print()
            terminal_print(
                f"Can't run workflow, the target files already exist, and '--output force=true' not specified: {target_file}"
            )

            sys.exit(1)

    api: BaseAPI = ctx.obj.base_api  # type: ignore

    cmd_arg = ctx.params["module_or_operation"]
    cmd_help = f"[yellow bold]Usage: [/yellow bold][bold]kiara run [OPTIONS] [i]{cmd_arg}[/i] [INPUTS][/bold]"

    # if module_config:
    #     op: Union[str, Mapping[str, Any]] = {
    #         "module_type": module_or_operation,
    #         "module_config": module_config,
    #     }
    # else:
    #     op = module_or_operation

    # base_inputs: Union[Mapping[str, Any], None] = None
    # extra_save: Union[None, Dict[str, str]] = None
    run_type = None
    job_descs: List[JobDesc] = []

    if not module_config and os.path.isfile(module_or_operation):

        path: Path = Path(module_or_operation)
        data = get_data_from_file(path)
        repl_dict: Dict[str, Any] = {"this_dir": path.parent.absolute().as_posix()}
        alias = path.stem

        if isinstance(data, list):
            raise NotImplementedError()
        elif isinstance(data, Mapping):
            if "operation" in data.keys():
                run_type = "job"
                job_desc = JobDesc.create_from_data(
                    data, var_repl_dict=repl_dict, alias=alias
                )
                job_descs.append(job_desc)

            elif "jobs" in data.keys():
                run_type = "run"

                if inputs:
                    terminal_print()
                    terminal_print(
                        "Can't specify inputs when running file with a run spec."
                    )
                    sys.exit(1)

                run_desc = RunSpec.create_from_data(
                    data, var_repl_dict=repl_dict, alias=alias
                )
                job_descs.extend(run_desc.jobs)
            elif "steps" not in data.keys():

                terminal_print()
                terminal_print(
                    f"Can't run file '{path}', it does not contain a valid pipeline, job or run specification."
                )
                sys.exit(1)
            else:
                # TODO: check if valid pipeline, otherwise check if 'module_or_operation is an operation name

                from kiara.models.module.jobs import ExecutionContext
                from kiara.models.module.pipeline import PipelineConfig

                pipeline_dir = os.path.abspath(os.path.dirname(path))
                execution_context = ExecutionContext(pipeline_dir=pipeline_dir)
                pc = PipelineConfig.from_config(
                    data, execution_context=execution_context, kiara=api.context
                )
                job_desc = JobDesc(
                    operation="pipeline",
                    module_config=pc.model_dump(),
                    job_alias="local_pipeline",
                )
                job_descs.append(job_desc)

    else:
        if module_config:
            job_desc = JobDesc(
                operation=module_or_operation,
                module_config=module_config,
                job_alias="default",
            )
        else:
            job_desc = JobDesc(operation=module_or_operation, job_alias="default")

        job_descs.append(job_desc)

    assert len(job_descs) > 0

    for job_desc in job_descs:

        if job_desc.module_config:
            op: Union[str, Mapping[str, Any]] = {
                "module_type": job_desc.operation,
                "module_config": job_desc.module_config,
            }
        else:
            op = job_desc.operation

        try:
            kiara_op = validate_operation_in_terminal(api=api, module_or_operation=op)
        except InvalidCommandLineInvocation as e:
            ctx.obj.exit(msg=None, exit_code=e.error_code)
            return

        log_message(f"run_arg.is.{run_type}")

        final_aliases = calculate_aliases(
            operation=kiara_op, alias_tokens=save, extra_aliases=job_desc.save
        )

        try:
            inputs_value_map = set_and_validate_inputs(
                api=api,
                operation=kiara_op,
                inputs=inputs,
                explain=explain,
                print_help=help,
                click_context=ctx,
                cmd_help=cmd_help,
                base_inputs=job_desc.inputs,
            )
            if inputs_value_map is None:
                ctx.obj.exit(msg=None, exit_code=0)
                return
        except InvalidCommandLineInvocation as e:
            ctx.obj.exit(msg=None, exit_code=e.error_code)
            return

        execute_job(
            api=api,
            operation=kiara_op,
            inputs=inputs_value_map,
            comment=comment,
            silent=silent,
            save_results=bool(final_aliases),
            aliases=final_aliases,
            properties=print_properties,
        )
