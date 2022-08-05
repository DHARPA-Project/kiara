# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Module related subcommands for the cli."""

import rich_click as click
from typing import Any, Iterable

# from kiara.interfaces.cli.utils import _create_module_instance
from kiara.interfaces.python_api import KiaraAPI
from kiara.utils.cli import (
    dict_from_cli_args,
    output_format_option,
    terminal_print_model,
)


@click.group()
@click.pass_context
def module(ctx):
    """Module-related sub-commands."""


@module.command(name="list")
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full documentation for every module type (when using 'terminal' output format).",
)
@output_format_option()
@click.argument("filter", nargs=-1, required=False)
@click.pass_context
def list_modules(ctx, full_doc: bool, filter: Iterable[str], format: str):
    """List available module data_types."""

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]
    module_types_info = kiara_api.retrieve_module_types_info(filter=filter)

    if filter:
        title = f"Filtered modules: {filter}"
    else:
        title = "All modules"

    terminal_print_model(
        module_types_info, format=format, in_panel=title, full_doc=full_doc
    )


@module.command(name="explain")
@click.argument("module_type", nargs=1, required=True)
@output_format_option()
@click.pass_context
def explain_module_type(ctx, module_type: str, format: str):
    """Print details of a module type.

    This is different to the 'explain-instance' command, because module data_types need to be
    instantiated with configuration, before we can query all their properties (like
    input/output data_types).
    """

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]
    info = kiara_api.retrieve_module_type_info(module_type=module_type)

    terminal_print_model(
        info, format=format, in_panel=f"Module type: [b i]{module_type}[/b i]"
    )


@module.command("explain-instance")
@click.argument("module_type", nargs=1)
@click.argument(
    "module_config",
    nargs=-1,
)
@output_format_option()
@click.pass_context
def explain_module(ctx, module_type: str, module_config: Iterable[Any], format: str):
    """Describe a module instance.

    This command shows information and metadata about an instantiated *kiara* module.
    """

    if module_config:
        module_config = dict_from_cli_args(*module_config)
    else:
        module_config = {}

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]

    operation = kiara_api.create_operation(
        module_type=module_type, module_config=module_config
    )

    terminal_print_model(
        operation.create_renderable(),  # type: ignore
        format=format,
        in_panel=f"Module instance of type: [b i]{module_type}[/b i]",
    )
