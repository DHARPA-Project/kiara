# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Pipeline-related subcommands for the cli."""
import sys
import typing
from pathlib import Path
from typing import Any, Mapping, Set, Tuple, Union

import rich_click as click
from rich.markdown import Markdown

from kiara.utils.cli import dict_from_cli_args, terminal_print, terminal_print_model
from kiara.utils.cli.exceptions import handle_exception

if typing.TYPE_CHECKING:
    from kiara.api import KiaraAPI


def render_wrapper(
    kiara_api: "KiaraAPI",
    source_type: str,
    item: Any,
    target_type: Union[str, None],
    render_config: Mapping[str, Any],
):

    if target_type is None:
        renderers = kiara_api.retrieve_renderers_for(source_type=source_type)
        all_targets: Set[str] = set()
        for renderer in renderers:
            targets = renderer.retrieve_supported_render_targets()
            if isinstance(targets, str):
                targets = [targets]
            all_targets.update(targets)

        terminal_print()
        msg = "No target type specified, available targets:\n\n"
        for target in all_targets:
            msg += f"- {target}\n"
        terminal_print(Markdown(msg))
        sys.exit(1)

    result = kiara_api.render(
        source_type=source_type,
        item=item,
        target_type=target_type,
        render_config=render_config,
    )
    return result


def result_wrapper(
    result: Any,
    output: Union[str, None],
    force: bool = False,
    terminal_render_config: Union[None, Mapping[str, Any]] = None,
):

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
            if terminal_render_config is None:
                terminal_render_config = {}
            terminal_print(result, **terminal_render_config)


@click.group()
@click.pass_context
def render(ctx) -> None:
    """Rendering-related sub-commands."""


@render.command()
@click.pass_context
def list_renderers(ctx) -> None:
    """List all available renderers."""
    kiara_api: KiaraAPI = ctx.obj.kiara_api

    infos = kiara_api.retrieve_renderer_infos()
    terminal_print()
    terminal_print_model(infos)


@render.group()
@click.argument("pipeline", nargs=1, metavar="PIPELINE_NAME_OR_PATH")
@click.pass_context
def pipeline(ctx, pipeline: str) -> None:
    """Render a kiara pipeline."""
    api: KiaraAPI = ctx.obj.kiara_api

    if pipeline.startswith("workflow:"):
        # pipeline_defaults = {}
        raise NotImplementedError()
    else:
        from kiara.models.module.pipeline.pipeline import Pipeline

        pipeline_obj = Pipeline.create_pipeline(kiara=api.context, pipeline=pipeline)

    ctx.obj.add_item("pipeline", pipeline_obj)


@pipeline.command("as")
@click.argument("target_type", nargs=1, metavar="TARGET_TYPE", required=False)
@click.argument("render_config", nargs=-1, required=False)
@click.option("--output", "-o", help="Write the rendered output to a file.")
@click.option("--force", "-f", help="Overwrite existing output file.", is_flag=True)
@click.pass_context
@handle_exception()
def render_func_pipeline(
    ctx,
    render_config: Tuple[str, ...],
    target_type: Union[str, None],
    output: Union[str, None],
    force: bool,
) -> None:

    kiara_api: KiaraAPI = ctx.obj.kiara_api
    item = ctx.obj.get_item("pipeline")

    render_config_dict = dict_from_cli_args(*render_config)

    result = render_wrapper(
        kiara_api=kiara_api,
        source_type="pipeline",
        item=item,
        target_type=target_type,
        render_config=render_config_dict,
    )

    result_wrapper(result=result, output=output, force=force)


@render.group()
@click.argument("value", nargs=1, metavar="VALUE_ID_OR_ALIAS")
@click.pass_context
def value(ctx, value: str) -> None:
    """Render a kiara value."""
    api: KiaraAPI = ctx.obj.kiara_api

    value_obj = api.get_value(value)

    ctx.obj.add_item("value", value_obj)


@value.command("as")
@click.argument("target_type", nargs=1, metavar="TARGET_TYPE", required=False)
@click.argument("render_config", nargs=-1, required=False)
@click.option("--output", "-o", help="Write the rendered output to a file.")
@click.option("--force", "-f", help="Overwrite existing output file.", is_flag=True)
# @click.option(
#     "--metadata",
#     "-m",
#     help="Also show the render metadata.",
#     is_flag=True,
#     default=False,
# )
# @click.option(
#     "--no-data", "-n", help="Show the rendered data.", is_flag=True, default=False
# )
@click.pass_context
@handle_exception()
def render_func_value(
    ctx,
    render_config: Tuple[str, ...],
    target_type: Union[str, None],
    output: Union[str, None],
    force: bool,
) -> None:

    kiara_api: KiaraAPI = ctx.obj.kiara_api
    item = ctx.obj.get_item("value")

    render_config_dict = dict_from_cli_args(*render_config)

    result = render_wrapper(
        kiara_api=kiara_api,
        source_type="value",
        item=item,
        target_type=target_type,
        render_config=render_config_dict,
    )

    # in case we have a rendervalue result, and we want to terminal print, we need to forward some of the render config
    show_render_metadata = render_config_dict.get("include_metadata", False)
    show_render_result = render_config_dict.get("include_data", True)
    cnf = {
        "show_render_metadata": show_render_metadata,
        "show_render_result": show_render_result,
    }

    result_wrapper(
        result=result, output=output, force=force, terminal_render_config=cnf
    )


@render.group("kiara_api")
@click.pass_context
def kiara_api(ctx) -> None:
    """Render a kiara value."""


@kiara_api.command("as")
@click.argument("target_type", nargs=1, metavar="TARGET_TYPE", required=False)
@click.argument("render_config", nargs=-1, required=False)
@click.option("--output", "-o", help="Write the rendered output to a file.")
@click.option("--force", "-f", help="Overwrite existing output file.", is_flag=True)
@click.pass_context
@handle_exception()
def render_func_api(
    ctx,
    render_config: Tuple[str, ...],
    target_type: Union[str, None],
    output: Union[str, None],
    force: bool,
) -> None:

    kiara_api: KiaraAPI = ctx.obj.kiara_api

    render_config_dict = dict_from_cli_args(*render_config)

    result = render_wrapper(
        kiara_api=kiara_api,
        source_type="kiara_api",
        item=kiara_api,
        target_type=target_type,
        render_config=render_config_dict,
    )

    # in case we have a rendervalue result, and we want to terminal print, we need to forward some of the render config
    show_render_metadata = render_config_dict.get("include_metadata", False)
    show_render_result = render_config_dict.get("include_data", True)
    cnf = {
        "show_render_metadata": show_render_metadata,
        "show_render_result": show_render_result,
    }

    result_wrapper(
        result=result, output=output, force=force, terminal_render_config=cnf
    )
