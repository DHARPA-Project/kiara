# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Data-related sub-commands for the cli."""
import rich_click as click
import structlog

from kiara import Kiara
from kiara.utils import StringYAML
from kiara.utils.cli import terminal_print
from kiara.workflows import Workflow

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

    print(kiara)


@workflow.command()
@click.argument("workflow_alias", nargs=1)
@click.pass_context
def create(ctx, workflow_alias):
    """Create a new workflow."""

    kiara: Kiara = ctx.obj["kiara"]
    # kiara.workflow_registry.create(workflow_alias)

    workflow = Workflow(kiara=kiara, workflow_alias=workflow_alias)
    add_step_id = workflow.add_step("logic.and")
    not_step_id = workflow.add_step("logic.not")

    workflow.add_input_link(f"{not_step_id}.a", f"{add_step_id}.y")

    terminal_print(workflow)
