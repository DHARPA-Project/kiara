# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Data-related sub-commands for the cli."""
import rich_click as click
import structlog
from typing import Tuple, Union

from kiara import Kiara
from kiara.interfaces.python_api.operation import KiaraOperation
from kiara.interfaces.python_api.workflow import Workflow
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.workflow import WorkflowInfo
from kiara.modules.included_core_modules.pipeline import PipelineModule
from kiara.utils import StringYAML, dict_from_cli_args
from kiara.utils.cli import terminal_print, terminal_print_model

logger = structlog.getLogger()

yaml = StringYAML()


@click.group()
@click.pass_context
def workflow(ctx):
    """Workflow-related sub-commands."""


@workflow.command()
@click.pass_context
def list(ctx):
    """List existing workflows."""

    kiara: Kiara = ctx.obj["kiara"]

    workflow_aliases = kiara.workflow_registry.workflow_aliases

    print(workflow_aliases)


@workflow.command()
@click.argument("workflow_alias", nargs=1)
@click.argument("blueprint", nargs=1, required=False)
@click.option(
    "--desc", "-d", help="Description string for the workflow.", required=False
)
@click.pass_context
def create(ctx, workflow_alias: str, blueprint: str, desc: Union[str, None] = None):
    """Create a new workflow."""

    kiara: Kiara = ctx.obj["kiara"]

    operation: Union[None, KiaraOperation] = None
    if blueprint:
        operation = KiaraOperation(kiara=kiara, operation_name=blueprint)

    workflow_details = kiara.workflow_registry.register_workflow(
        workflow_aliases=[workflow_alias], workflow_details=desc
    )

    workflow_details.create_workflow_state()

    if blueprint:
        assert operation

        module = operation.operation.module
        if isinstance(module, PipelineModule):
            config: PipelineConfig = module.config
        else:
            raise NotImplementedError()

        state = workflow_details.create_workflow_state(steps=config.steps)

        workflow_details = kiara.workflow_registry.add_workflow_state(
            workflow=workflow_details, workflow_state=state, set_current=True
        )

    terminal_print_model(workflow_details)


@workflow.command()
@click.argument("workflow", nargs=1)
@click.pass_context
def explain(ctx, workflow: str):
    """Explain the workflow with the specified id/alias."""

    kiara: Kiara = ctx.obj["kiara"]
    # workflow_obj = kiara.workflow_registry.get_workflow_details(workflow=workflow)
    # terminal_print_model(workflow_obj)
    # state = kiara.workflow_registry.get_workflow_state(workflow=workflow)
    # dbg(state)
    workflow_details = kiara.workflow_registry.get_workflow_details(workflow=workflow)

    workflow_obj = Workflow(kiara=kiara, workflow=workflow_details.workflow_id)

    workflow_info = WorkflowInfo.create_from_workflow(workflow=workflow_obj)
    terminal_print_model(workflow_info)


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

    workflow_obj = Workflow(kiara=kiara, workflow=workflow)

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
    terminal_print(workflow_obj)

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
