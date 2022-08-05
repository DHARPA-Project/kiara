# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Data-related sub-commands for the cli."""
import rich_click as click
import structlog
from typing import Tuple, Union

from kiara.context import Kiara
from kiara.interfaces.python_api.workflow import Workflow
from kiara.models.workflow import WorkflowGroupInfo
from kiara.utils.cli import dict_from_cli_args, terminal_print, terminal_print_model
from kiara.utils.yaml import StringYAML

logger = structlog.getLogger()

yaml = StringYAML()


@click.group()
@click.pass_context
def workflow(ctx):
    """Workflow-related sub-commands."""


@workflow.command()
@click.option(
    "--all", "-a", help="Also displays workflows without alias.", is_flag=True
)
@click.pass_context
def list(ctx, all):
    """List existing workflows."""

    kiara: Kiara = ctx.obj["kiara"]

    workflows = []

    for workflow_id in kiara.workflow_registry.all_workflow_ids:

        workflow = Workflow(kiara=kiara, workflow=workflow_id)
        workflows.append(workflow)

    all_aliases = kiara.workflow_registry.workflow_aliases

    workflow_infos = WorkflowGroupInfo.create_from_workflows(
        *workflows, group_alias=None, alias_map=all_aliases
    )

    terminal_print_model(workflow_infos)


@workflow.command()
@click.argument("workflow_alias", nargs=1)
@click.argument("blueprint", nargs=1, required=False)
@click.option(
    "--desc", "-d", help="Description string for the workflow.", required=False
)
@click.pass_context
def create(
    ctx, workflow_alias: str, blueprint: Union[str, None], desc: Union[str, None] = None
):
    """Create a new workflow."""

    kiara: Kiara = ctx.obj["kiara"]

    workflow_obj = Workflow.create(
        alias=workflow_alias, blueprint=blueprint, kiara=kiara
    )

    workflow_obj.process_steps()

    workflow_obj.snapshot()

    terminal_print_model(
        workflow_obj.info.create_renderable(),
        in_panel=f"Workflow: [b i]{workflow_alias}[/b i]",
    )


@workflow.command()
@click.argument("workflow", nargs=1)
@click.option(
    "--states", "-s", help="Display the history of this workflows states.", is_flag=True
)
@click.pass_context
def explain(ctx, workflow: str, states: bool):
    """Explain the workflow with the specified id/alias."""

    kiara: Kiara = ctx.obj["kiara"]

    workflow_details = kiara.workflow_registry.get_workflow_details(workflow=workflow)

    workflow_obj = Workflow(kiara=kiara, workflow=workflow_details.workflow_id)
    terminal_print(
        workflow_obj.info.create_renderable(include_history=states),
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

    kiara: Kiara = ctx.obj["kiara"]

    workflow_details = kiara.workflow_registry.get_workflow_details(workflow=workflow)
    workflow_obj = Workflow(kiara=kiara, workflow=workflow_details.workflow_id)

    inputs_schema = workflow_obj.current_inputs_schema
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
            print(e)

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
