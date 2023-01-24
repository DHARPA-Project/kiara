# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Pipeline-related subcommands for the cli."""
import sys
from typing import Tuple, Union

import rich_click as click
from rich.markdown import Markdown

from kiara import KiaraAPI
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.utils.cli import dict_from_cli_args, terminal_print
from kiara.utils.cli.exceptions import handle_exception


@click.group()
@click.pass_context
def render(ctx) -> None:
    """Rendering-related sub-commands."""


@render.group()
@click.argument("pipeline", nargs=1, metavar="PIPELINE_NAME_OR_PATH")
@click.pass_context
def pipeline(ctx, pipeline: str) -> None:
    api: KiaraAPI = ctx.obj["kiara_api"]

    if pipeline.startswith("workflow:"):
        # pipeline_defaults = {}
        raise NotImplementedError()
    else:
        pipeline_obj = Pipeline.create_pipeline(kiara=api.context, pipeline=pipeline)

    ctx.obj["item"] = pipeline_obj


@pipeline.command("as")
@click.argument("renderer", nargs=1, metavar="RENDERER_NAME", required=False)
@click.argument("render_config", nargs=-1, required=False)
@click.pass_context
@handle_exception()
def render_func(
    ctx, render_config: Tuple[str, ...], renderer: Union[str, None]
) -> None:

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]
    item = ctx.obj["item"]

    if not renderer:
        avaialable_renderers = kiara_api.retrieve_renderer_names_for(item)

        msg = "No renderer specified. Available renderers for 'pipeline':\n\n"
        for renderer in avaialable_renderers:
            msg += f" - {renderer}\n"
        terminal_print()
        terminal_print(Markdown(msg), in_panel="Missing rendererl")
        sys.exit(1)

    render_config_dict = dict_from_cli_args(*render_config)

    result = kiara_api.render(
        item, renderer_name=renderer, render_config=render_config_dict
    )

    print(result)  # noqa


# @pipeline.command()
# @click.argument("base_name", nargs=1, required=False)
# @click.option("--execution-graph", is_flag=True, help="Render the execution graph.")
# @click.option(
#     "--data-flow-graph", is_flag=True, help="Render the data-flow-graph graph."
# )
# @click.option(
#     "--data-flow-graph-simple",
#     is_flag=True,
#     help="Render the simplified data-flow-graph graph.",
# )
# @click.option(
#     "--output-dir",
#     type=click.Path(dir_okay=True, file_okay=False),
#     help="Output directory.",
#     required=False,
# )
# @click.pass_context
# def as_graph_images(
#     ctx,
#     base_name: Union[str, None],
#     execution_graph: bool,
#     data_flow_graph: bool,
#     data_flow_graph_simple: bool,
#     output_dir: Union[None, str],
# ) -> None:
#
#     from kiara.utils.graphs import save_image
#
#     pipeline_obj: Pipeline = ctx.obj["pipeline"]
#
#     if base_name is None:
#         base_name = pipeline_obj.structure.pipeline_config.pipeline_name
#
#     if not execution_graph and not data_flow_graph and not data_flow_graph_simple:
#         execution_graph = True
#         data_flow_graph = True
#         data_flow_graph_simple = True
#
#     if output_dir is None:
#         output_dir = os.getcwd()
#
#     os.makedirs(output_dir, exist_ok=True)
#
#     if execution_graph:
#         path = os.path.join(output_dir, f"{base_name}-execution-graph.png")
#         save_image(graph=pipeline_obj.execution_graph, path=path)
#
#     if data_flow_graph:
#         path = os.path.join(output_dir, f"{base_name}-data-flow-graph.png")
#         save_image(graph=pipeline_obj.data_flow_graph, path=path)
#
#     if data_flow_graph_simple:
#         path = os.path.join(output_dir, f"{base_name}-data-flow-graph-simplified.png")
#         save_image(graph=pipeline_obj.data_flow_graph_simple, path=path)
#
#
# @pipeline.command()
# @click.argument("inputs", nargs=-1, required=False)
# @click.option("--template", "-t", default="notebook")
# @click.pass_context
# def from_template(ctx, template: str, inputs: Tuple[str]):
#     """Render a pipeline into a notebook, streamlit app, etc...
#
#     This command is still work in progress, and its interface will likely change in the future.
#     """
#
#     kiara: Kiara = ctx.obj["kiara"]
#
#     # if template == "notebook":
#     #
#     #     my_resources = importlib_resources.files("kiara")
#     #     template_path = (
#     #         my_resources
#     #         / "resources"
#     #         / "templates"
#     #         / "render"
#     #         / "pipeline"
#     #         / "workflow_tutorial"
#     #         / "jupyter_notebook.ipynb.j2"
#     #     )
#     #
#     #     template_content = template_path.read_text()
#     #
#     # elif os.path.isfile(template):
#     #     template_path = Path(template)
#     #     # template_content = template_path.read_text()
#     # else:
#     #     terminal_print()
#     #     terminal_print(
#     #         "Invalid value for 'template': must be one of 'notebook', 'script', or a path to a jinja template file."
#     #     )
#     #     sys.exit(1)
#
#     pipeline_obj = ctx.obj["pipeline"]
#     # controller = SinglePipelineBatchController(pipeline=pipeline, job_registry=kiara.job_registry)
#     pipeline_defaults = pipeline_obj.structure.pipeline_config.defaults
#
#     pipeline_inputs = dict(pipeline_defaults)
#     if inputs:
#         # prepare inputs
#         list_keys = []
#         for name, value_schema in pipeline_obj.structure.pipeline_inputs_schema.items():
#             if value_schema.type in ["list"]:
#                 list_keys.append(name)
#
#         inputs_dict = dict_from_cli_args(*inputs, list_keys=list_keys)
#         pipeline_inputs.update(inputs_dict)
#
#     pipeline_obj.set_pipeline_inputs(inputs=pipeline_inputs)
#
#     if template == "notebook":
#
#         pkg_path = os.path.join(
#             "resources", "templates", "render", "pipeline", "workflow_tutorial"
#         )
#         loader = {
#             "loader_type": "package",
#             "loader_conf": {"package_name": "kiara", "package_path": pkg_path},
#         }
#
#         config = {"template": "jupyter_notebook.ipynb.j2", "loader": loader}
#         renderer = JinjaPipelineRenderer(config=config, kiara=kiara)
#     elif os.path.isfile(template):
#         raise NotImplementedError()
#     else:
#         terminal_print()
#         terminal_print(
#             "Invalid value for 'template': must be one of 'notebook', 'script', or a path to a jinja template file."
#         )
#         sys.exit(1)
#
#     rendered = renderer.render(pipeline_obj)
#
#     print(rendered)
