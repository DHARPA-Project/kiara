# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click

from kiara.utils.cli import output_format_option, terminal_print_model
from kiara.utils.cli.exceptions import handle_exception

if TYPE_CHECKING:
    from kiara.interfaces import BaseAPI, BaseAPIWrap


@click.group("info")
@click.pass_context
def info(ctx):
    """Kiara config related sub-commands."""


@info.group("config")
@click.pass_context
def config(ctx):
    """Kiara config related sub-commands."""


@config.command("print")
@output_format_option()
@click.pass_context
def print_config(ctx, format: str):

    from kiara.context import KiaraConfig

    wrap: "BaseAPIWrap" = ctx.obj
    config: KiaraConfig = wrap.kiara_config
    title = "kiara config"
    if config._config_path:
        title = f"{title} - [i]{config._config_path}[/i]"

    terminal_print_model(config, format=format, in_panel=title)


@info.group("plugin")
@click.pass_context
def plugin(ctx):
    """Kiara plugin related sub-commands."""


@plugin.command("list")
@click.argument("filter-regex", nargs=1, required=False)
@output_format_option()
@click.pass_context
def list_plugins(ctx, filter_regex: str, format):
    """List installed kiara plugins."""

    from kiara.interfaces.python_api.models.info import KiaraPluginInfos

    api: BaseAPI = ctx.obj.base_api

    title = "All available plugins"
    if filter_regex:
        title = "Matching plugins"

    plugin_infos = KiaraPluginInfos.create_group(api.context, title, filter_regex)

    terminal_print_model(plugin_infos, format=format, in_panel=title)


@plugin.command("explain")
@click.argument("plugin_name", nargs=1)
@output_format_option()
@handle_exception()
@click.pass_context
def explain_plugin_info(ctx, plugin_name: str, format: str):

    kiara_api: BaseAPI = ctx.obj.base_api

    plugin_info = kiara_api.retrieve_plugin_info(plugin_name)
    title = f"Info for plugin: [i]{plugin_name}[/i]"

    terminal_print_model(plugin_info, format=format, in_panel=title)
