# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Pipeline-related subcommands for the cli."""
import importlib.resources
import os.path
import rich_click as click
import sys
from typing import Tuple

from kiara.context import Kiara
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.render.pipeline import JinjaPipelineRenderer
from kiara.utils.cli import dict_from_cli_args, terminal_print


@click.group()
@click.pass_context
def render(ctx) -> None:
    """Rendering-related sub-commands."""


@render.group()
@click.argument("pipeline", nargs=1)
@click.pass_context
def pipeline(ctx, pipeline: str) -> None:
    kiara: Kiara = ctx.obj["kiara"]

    if pipeline.startswith("workflow:"):
        # pipeline_defaults = {}
        raise NotImplementedError()
    else:
        pipeline_obj = Pipeline.create_pipeline(kiara=kiara, pipeline=pipeline)

    ctx.obj["pipeline"] = pipeline_obj


@pipeline.command()
@click.argument("base_name", nargs=1)
@click.pass_context
def as_graph_images(ctx, base_name: str) -> None:

    from kiara.utils.jupyter import save_image

    pipeline_obj: Pipeline = ctx.obj["pipeline"]

    path = os.path.join(os.getcwd(), f"{base_name}-execution-graph.png")
    save_image(graph=pipeline_obj.execution_graph, path=path)

    path = os.path.join(os.getcwd(), f"{base_name}-data-flow-graph.png")
    save_image(graph=pipeline_obj.data_flow_graph, path=path)

    path = os.path.join(os.getcwd(), f"{base_name}-data-flow-graph-simplified.png")
    save_image(graph=pipeline_obj.data_flow_graph_simple, path=path)


@pipeline.command()
@click.argument("inputs", nargs=-1, required=False)
@click.option("--template", "-t", default="notebook")
@click.pass_context
def from_template(ctx, template: str, inputs: Tuple[str]):
    """Render a pipeline into a notebook, streamlit app, etc...

    This command is still work in progress, and its interface will likely change in the future.
    """

    kiara: Kiara = ctx.obj["kiara"]

    if template == "notebook":
        template_path = importlib.resources.read_text(
            "kiara",
            os.path.join(
                "templates",
                "render",
                "pipeline",
                "workflow_tutorial",
                "jupyter_notebook.ipynb.j2",
            ),
        )
        # template_path = os.path.join(
        #     KIARA_RESOURCES_FOLDER,
        #     "templates",
        #     "render",
        #     "pipeline",
        #     "workflow_tutorial",
        #     "jupyter_notebook.ipynb.j2",
        # )
    elif os.path.isfile(template):
        template_path = template
    else:
        terminal_print()
        terminal_print(
            "Invalid value for 'template': must be one of 'notebook', 'script', or a path to a jinja template file."
        )
        sys.exit(1)

    pipeline_obj = ctx.obj["pipeline"]
    # controller = SinglePipelineBatchController(pipeline=pipeline, job_registry=kiara.job_registry)
    pipeline_defaults = pipeline_obj.structure.pipeline_config.defaults

    pipeline_inputs = dict(pipeline_defaults)
    if inputs:
        # prepare inputs
        list_keys = []
        for name, value_schema in pipeline_obj.structure.pipeline_inputs_schema.items():
            if value_schema.type in ["list"]:
                list_keys.append(name)

        inputs_dict = dict_from_cli_args(*inputs, list_keys=list_keys)
        pipeline_inputs.update(inputs_dict)

    pipeline_obj.set_pipeline_inputs(inputs=pipeline_inputs)

    render_config = {"template": template_path}
    renderer = JinjaPipelineRenderer(config=render_config, kiara=kiara)

    rendered = renderer.render(pipeline_obj)

    print(rendered)
