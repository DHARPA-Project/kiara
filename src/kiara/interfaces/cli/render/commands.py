# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Pipeline-related subcommands for the cli."""

import os.path
import rich_click as click
import sys
from typing import Tuple

from kiara.context import Kiara
from kiara.defaults import KIARA_RESOURCES_FOLDER
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.render.pipeline import JinjaPipelineRenderer
from kiara.utils.cli import dict_from_cli_args, terminal_print


@click.group()
@click.pass_context
def render(ctx):
    """Rendering-related sub-commands."""


@render.command()
@click.argument("pipeline", nargs=1)
@click.argument("inputs", nargs=-1, required=False)
@click.option("--template", "-t", default="notebook")
@click.pass_context
def pipeline(ctx, pipeline: str, template: str, inputs: Tuple[str]):
    """Render a pipeline into a notebook, streamlit app, etc...

    This command is still work in progress, and its interface will likely change in the future.
    """

    kiara: Kiara = ctx.obj["kiara"]

    if template == "notebook":
        template_path = os.path.join(
            KIARA_RESOURCES_FOLDER,
            "templates",
            "render",
            "pipeline",
            "workflow_tutorial",
            "jupyter_notebook.ipynb.j2",
        )
    elif os.path.isfile(template):
        template_path = template
    else:
        terminal_print()
        terminal_print(
            "Invalid value for 'template': must be one of 'notebook', 'script', or a path to a jinja template file."
        )
        sys.exit(1)

    if pipeline.startswith("workflow:"):
        raise NotImplementedError()
    else:
        pipeline_obj = Pipeline.create_pipeline(kiara=kiara, pipeline=pipeline)
        # controller = SinglePipelineBatchController(pipeline=pipeline, job_registry=kiara.job_registry)

    if inputs:
        # prepare inputs
        list_keys = []
        for name, value_schema in pipeline_obj.structure.pipeline_inputs_schema.items():
            if value_schema.type in ["list"]:
                list_keys.append(name)

        inputs_dict = dict_from_cli_args(*inputs, list_keys=list_keys)
        pipeline_obj.set_pipeline_inputs(inputs=inputs_dict)

    render_config = {"template": template_path}
    renderer = JinjaPipelineRenderer(config=render_config, kiara=kiara)

    rendered = renderer.render(pipeline_obj)

    print(rendered)
