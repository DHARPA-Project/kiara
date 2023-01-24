# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Pipeline-related subcommands for the cli."""
import sys
from pathlib import Path
from typing import Any, Mapping, Tuple, Union

import rich_click as click
from rich.markdown import Markdown
from rich.tree import Tree

from kiara import KiaraAPI
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.utils.cli import dict_from_cli_args, terminal_print
from kiara.utils.cli.exceptions import handle_exception


def render_wrapper(
    kiara_api: KiaraAPI,
    item_type: str,
    item: Any,
    renderer: Union[str, None],
    render_config: Mapping[str, Any],
):

    if not renderer:
        avaialable_renderers = kiara_api.retrieve_renderer_names_for(item)

        msg = f"No renderer specified. Available renderers for '{item_type}':\n\n"
        for renderer in avaialable_renderers:
            msg += f" - {renderer}\n"
        terminal_print()
        terminal_print(Markdown(msg), in_panel="Missing renderer")
        sys.exit(1)

    result = kiara_api.render(item, renderer_name=renderer, render_config=render_config)
    return result


def result_wrapper(result: Any, output: Union[str, None], force: bool = False):

    if output:
        output_file = Path(output)
        if output_file.exists() and not force:
            terminal_print()
            terminal_print(
                f"Output file '{output_file}' already exists, use '--force' to overwrite.",
            )
        output_file.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(result, str):
            output_file.write_text(result)
        elif isinstance(result, bytes):
            output_file.write_bytes(result)
        else:
            terminal_print()
            terminal_print(
                f"Render output if type '{type(result)}', can't write to file."
            )

    else:

        if isinstance(result, str):
            print(result)  # noqa
        elif isinstance(result, bytes):
            terminal_print()
            terminal_print(
                "Render result is binary data, can't print to terminal. Use the '--output' option to write to a file."
            )
        else:
            terminal_print(result)


@click.group()
@click.pass_context
def render(ctx) -> None:
    """Rendering-related sub-commands."""


@render.command()
@click.pass_context
def list_renderers(ctx):
    """List all available renderers."""

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]

    tree = Tree("[b i]Renderers[/b i]")
    for (
        source_type,
        renderers,
    ) in kiara_api.context.render_registry.registered_renderers.items():
        source_node = tree.add(f"Source type: [b]{source_type.__name__}[/b]")
        for renderer_name, renderer in renderers.items():
            source_node.add(f"renderer: [i]{renderer_name}[/i]")

    terminal_print()
    terminal_print(tree)


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
@click.option("--output", "-o", help="Write the rendered output to a file.")
@click.option("--force", "-f", help="Overwrite existing output file.", is_flag=True)
@click.pass_context
@handle_exception()
def render_func_pipeline(
    ctx,
    render_config: Tuple[str, ...],
    renderer: Union[str, None],
    output: Union[str, None],
    force: bool,
) -> None:

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]
    item = ctx.obj["item"]

    render_config_dict = dict_from_cli_args(*render_config)

    result = render_wrapper(
        kiara_api=kiara_api,
        item_type="pipeline",
        item=item,
        renderer=renderer,
        render_config=render_config_dict,
    )

    result_wrapper(result=result, output=output, force=force)


@render.group()
@click.argument("value", nargs=1, metavar="VALUE_ID_OR_ALIAS")
@click.pass_context
def value(ctx, value: str) -> None:
    api: KiaraAPI = ctx.obj["kiara_api"]

    value_obj = api.get_value(value)

    ctx.obj["item"] = value_obj


@value.command("as")
@click.argument("renderer", nargs=1, metavar="RENDERER_NAME", required=False)
@click.argument("render_config", nargs=-1, required=False)
@click.option("--output", "-o", help="Write the rendered output to a file.")
@click.option("--force", "-f", help="Overwrite existing output file.", is_flag=True)
@click.option(
    "--metadata",
    "-m",
    help="Also show the render metadata.",
    is_flag=True,
    default=False,
)
@click.option(
    "--no-data", "-n", help="Show the rendered data.", is_flag=True, default=False
)
@click.pass_context
@handle_exception()
def render_func_value(
    ctx,
    render_config: Tuple[str, ...],
    renderer: Union[str, None],
    metadata: bool,
    no_data: bool,
    output: Union[str, None],
    force: bool,
) -> None:

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]
    item = ctx.obj["item"]

    render_config_dict = dict_from_cli_args(*render_config)
    render_config_dict = {"render_config": render_config_dict}
    result = render_wrapper(
        kiara_api=kiara_api,
        item_type="value",
        item=item,
        renderer=renderer,
        render_config=render_config_dict,
    )

    if output:
        result_wrapper(result=result.rendered, output=output, force=force)
    else:
        conf = {"show_render_result": True, "show_render_metadata": False}
        if metadata:
            conf["show_render_metadata"] = True
        if no_data:
            conf["show_render_result"] = False

        terminal_print(result.create_renderable(**conf))
