# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import rich_click as click

from kiara.context import KiaraConfig
from kiara.utils.cli import output_format_option, terminal_print_model


@click.group("config")
@click.pass_context
def config(ctx):
    """Kiara config related sub-commands."""


@config.command("print")
@output_format_option()
@click.pass_context
def print_config(ctx, format: str):

    config: KiaraConfig = ctx.obj["kiara_config"]
    title = "kiara config"
    if config._config_path:
        title = f"{title} - [i]{config._config_path}[/i]"

    terminal_print_model(config, format=format, in_panel=title)
