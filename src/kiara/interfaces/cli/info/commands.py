# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click

from kiara.utils.cli import output_format_option, terminal_print_model

if TYPE_CHECKING:
    from kiara.interfaces import KiaraAPI, KiaraAPIWrap


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

    wrap: "KiaraAPIWrap" = ctx.obj
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
@click.pass_context
def list_plugins(ctx):
    """List installed kiara plugins."""


@plugin.command("print")
@click.argument("plugin_name", nargs=1)
@output_format_option()
@click.pass_context
def print_plugin_info(ctx, plugin_name: str, format: str):

    kiara_api: KiaraAPI = ctx.obj.kiara_api

    plugin_info = kiara_api.get_plugin_info(plugin_name)
    title = f"Info for plugin: [i]{plugin_name}[/i]"

    terminal_print_model(plugin_info, format=format, in_panel=title)
