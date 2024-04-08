# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Data-related sub-commands for the cli."""
import typing
from typing import Any, Dict, Tuple, Union

import rich_click as click
import structlog

from kiara.utils.cli import dict_from_cli_args, terminal_print, terminal_print_model

if typing.TYPE_CHECKING:
    from kiara.interfaces.python_api.base_api import BaseAPI, Kiara

logger = structlog.getLogger()


@click.group()
@click.pass_context
def workflow(ctx):
    """Workflow-related sub-commands.

    All of those are experiments at the moment, so don't expect them to be available in the future.
    """


@workflow.command()
@click.option(
    "--all", "-a", help="Also displays workflows without alias.", is_flag=True
)
@click.pass_context
def list(ctx, all) -> None:
    """List existing workflows."""
    kiara_api: BaseAPI = ctx.obj.base_api

    if all:
        workflows = kiara_api.retrieve_workflows_info()
    else:
        workflows = kiara_api.retrieve_workflow_aliases_info()

    terminal_print_model(workflows)


@workflow.command()
@click.argument("workflow_alias", nargs=1, required=False)
@click.argument("blueprint", nargs=1, required=False)
@click.argument("inputs", nargs=-1)
@click.option(
    "--force-alias",
    "-f",
    is_flag=True,
    help="Force (replace) an existing alias, if equal to the one provided.",
)
@click.option(
    "--desc", "-d", help="Description string for the workflow.", required=False
)
@click.pass_context
def create(
    ctx,
    workflow_alias: str,
    blueprint: Union[str, None],
    desc: Union[str, None] = None,
    inputs: Tuple[str, ...] = (),
    force_alias: bool = False,
):
    """Create a new workflow."""
    kiara_api: BaseAPI = ctx.obj.base_api

    inputs_dict: Union[None, Dict[str, Any]] = None
    if inputs:
        inputs_dict = dict_from_cli_args(*inputs)

    workflow_obj = kiara_api.create_workflow(
        workflow_alias=workflow_alias,
        initial_pipeline=blueprint,
        initial_inputs=inputs_dict,
    )

    workflow_obj.process_steps()

    if force_alias:
        kiara_api.context.workflow_registry.unregister_alias(workflow_alias)

    workflow_obj.snapshot(save=True)

    terminal_print_model(
        workflow_obj.info,
        in_panel=f"Workflow: [b i]{workflow_alias}[/b i]",
    )


@workflow.command()
@click.argument("workflow", nargs=1)
@click.pass_context
def explain(ctx, workflow: str):
    """Explain the workflow with the specified id/alias."""
    kiara_api: BaseAPI = ctx.obj.base_api
    workflow_info = kiara_api.retrieve_workflow_info(workflow=workflow)
    terminal_print(
        workflow_info.create_renderable(),
        in_panel=f"Workflow: [b i]{workflow}[/b i]",
    )


@workflow.command()
@click.argument("workflow", nargs=1)
@click.argument("inputs", nargs=-1, required=False)
@click.option(
    "--process/--no-process",
    "-a/-n",
    help="Process all possible intermediate and end-results.",
    is_flag=True,
    default=True,
)
@click.pass_context
def set_input(ctx, workflow: str, inputs: Tuple[str], process: bool):
    """Set one or several inputs on the specified workflow."""

    from kiara.interfaces.python_api.workflow import Workflow

    kiara: Kiara = ctx.obj.kiara

    workflow_details = kiara.workflow_registry.get_workflow_metadata(workflow=workflow)
    workflow_obj = Workflow(kiara=kiara, workflow=workflow_details.workflow_id)

    inputs_schema = workflow_obj.current_pipeline_inputs_schema
    list_keys = []
    for name, value_schema in inputs_schema.items():
        if value_schema.type in ["list"]:
            list_keys.append(name)
    inputs_dict = dict_from_cli_args(*inputs, list_keys=list_keys)

    workflow_obj.set_inputs(**inputs_dict)

    if process:
        try:
            workflow_obj.process_steps()
        except Exception as e:
            terminal_print(e)

    workflow_obj.snapshot(save=True)
    terminal_print_model(workflow_obj.info, in_panel=f"Workflow: [b i]{workflow}[/b i]")

    # workflow_obj.save_state()

    # registered = kiara.data_registry.create_valuemap(
    #     data=inputs_dict, schema=inputs_schema
    # )
    # for k, v in registered.items():
    #     kiara.data_registry.store_value(v)
    #
    # value_ids = registered.get_all_value_ids()
    #
    # new_state = state.create_workflow_state(inputs=value_ids)
    # kiara.workflow_registry.add_workflow_state(
    #     workflow_state=new_state, set_current=True
    # )
