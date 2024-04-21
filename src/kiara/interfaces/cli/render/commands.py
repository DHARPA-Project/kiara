# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Pipeline-related subcommands for the cli."""
import sys
import typing
from pathlib import Path
from typing import Tuple, Union

import rich_click as click
from rich.markdown import Markdown

from kiara.utils.cli import (
    dict_from_cli_args,
    output_format_option,
    terminal_print,
    terminal_print_model,
)
from kiara.utils.cli.exceptions import handle_exception

if typing.TYPE_CHECKING:
    from kiara.interfaces import BaseAPIWrap
    from kiara.interfaces.python_api.base_api import BaseAPI


# def list_renderers(ctx, param, value) -> None:
#     """List all available renderers."""
#
#     if not value or ctx.resilient_parsing:
#         return
#     kiara_api: KiaraAPI = ctx.obj.kiara_api
#
#     infos = kiara_api.retrieve_renderer_infos()
#     terminal_print()
#     terminal_print_model(infos, in_panel="Available renderers")
#     sys.exit(0)


@click.group(name="render")
@click.option(
    "--source-type", "-s", required=False, help="Source type of the item to render."
)
@click.option("--target-type", "-t", required=False, help="Target type to render to.")
@click.pass_context
def render(
    ctx, source_type: Union[None, str] = None, target_type: Union[None, str] = None
) -> None:
    """Render-related sub-commands."""

    api_wrap: BaseAPIWrap = ctx.obj
    api_wrap.add_item("source_type", source_type)
    api_wrap.add_item("target_type", target_type)


@render.command("list-renderers")
@output_format_option()
@click.pass_context
@handle_exception()
def list_render_combinations(ctx, format: str):

    api_wrap: BaseAPIWrap = ctx.obj
    kiara_api: BaseAPI = api_wrap.base_api

    source_type = api_wrap.get_item("source_type")
    target_type = api_wrap.get_item("target_type")

    infos = kiara_api.retrieve_renderer_infos(
        source_type=source_type, target_type=target_type
    )
    terminal_print()
    terminal_print_model(infos, in_panel="Available renderers", format=format)
    sys.exit(0)


@render.command(name="item")
# @click.option(
#     "--list",
#     "-l",
#     is_flag=True,
#     help="List all available renderers and exit.",
#     callback=list_renderers,
#     expose_value=False,
#     is_eager=True,
# )
@click.option(
    "--output", "-o", help="Write the rendered output to a file using this path."
)
@click.option("--force", "-f", help="Overwrite existing output file.", is_flag=True)
@click.argument("item_to_render", nargs=1)
@click.argument("render_config", nargs=-1)
@click.pass_context
@handle_exception()
def render_item(
    ctx,
    item_to_render: Union[str, None],
    render_config: Tuple[str],
    output: str,
    force: bool,
) -> None:
    """Render an internal kiara item."""

    api_wrap: BaseAPIWrap = ctx.obj
    kiara_api: BaseAPI = api_wrap.base_api

    source_type = api_wrap.get_item("source_type")
    target_type = api_wrap.get_item("target_type")

    infos = kiara_api.retrieve_renderer_infos()

    available_render_source_types = infos.get_render_source_types()

    if source_type is None:

        if item_to_render in available_render_source_types:
            source_type = item_to_render
        else:
            msg = "No render source type specified, available source types:\n\n"

            for source_type in available_render_source_types:
                msg = f"{msg}  - *{source_type}*\n"

            terminal_print()
            terminal_print(Markdown(msg))
            sys.exit(1)

    elif source_type not in available_render_source_types:
        msg = f"Render source type '{source_type}' not available, available source types:\n\n"

        for source_type in available_render_source_types:
            msg = f"{msg}  - *{source_type}*\n"

        terminal_print()
        terminal_print(Markdown(msg))
        sys.exit(1)

    renderers = kiara_api.retrieve_renderers_for(source_type=source_type)

    all_targets: typing.Set[str] = set()
    for renderer in renderers:
        targets = renderer.retrieve_supported_render_targets()
        if isinstance(targets, str):
            targets = [targets]
        all_targets.update(targets)

    if target_type is None:
        msg = "No render target type specified, available target types:\n\n"

        for target_type in sorted(all_targets):
            msg = f"{msg}  - *{target_type}*\n"

        terminal_print()
        terminal_print(Markdown(msg))
        sys.exit(1)

    if target_type not in all_targets:
        msg = f"Render target type '{target_type}' not available, available target types:\n\n"

        for target_type in sorted(all_targets):
            msg = f"{msg}  - *{target_type}*\n"

        terminal_print()
        terminal_print(Markdown(msg))
        sys.exit(1)

    render_config_dict = dict_from_cli_args(*render_config)

    result = kiara_api.render(
        item=item_to_render,
        source_type=source_type,
        target_type=target_type,
        render_config=render_config_dict,
    )

    # in case we have a rendervalue result, and we want to terminal print, we need to forward some of the render config
    show_render_metadata = render_config_dict.get("include_metadata", False)
    show_render_result = render_config_dict.get("include_data", True)
    terminal_render_config = {
        "show_render_metadata": show_render_metadata,
        "show_render_result": show_render_result,
    }

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
