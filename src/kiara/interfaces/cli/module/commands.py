# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Module related subcommands for the cli."""

from typing import TYPE_CHECKING, Any, Iterable, Union

import rich_click as click

from kiara.utils.cli import (
    dict_from_cli_args,
    output_format_option,
    terminal_print_model,
)

if TYPE_CHECKING:
    from kiara.interfaces.python_api.base_api import BaseAPI


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
@click.option(
    "--python-package",
    "-p",
    help="Only return modules from this package.",
    required=False,
)
@click.pass_context
def list_modules(
    ctx,
    full_doc: bool,
    filter: Iterable[str],
    format: str,
    python_package: Union[str, None],
):
    """List available module data_types."""
    kiara_api: BaseAPI = ctx.obj.base_api

    module_types_info = kiara_api.retrieve_module_types_info(
        filter=filter, python_package=python_package
    )

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
    """
    Print details of a module type.

    This is different to the 'explain-instance' command, because module data_types need to be
    instantiated with configuration, before we can query all their properties (like
    input/output data_types).
    """
    kiara_api: BaseAPI = ctx.obj.base_api
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
    """
    Describe a module instance.

    This command shows information and metadata about an instantiated *kiara* module.
    """
    if module_config:
        module_config = dict_from_cli_args(*module_config)
    else:
        module_config = {}

    kiara_api: BaseAPI = ctx.obj.base_api

    operation = kiara_api.create_operation(
        module_type=module_type, module_config=module_config
    )

    terminal_print_model(
        operation.create_renderable(),  # type: ignore
        format=format,
        in_panel=f"Module instance of type: [b i]{module_type}[/b i]",
    )
