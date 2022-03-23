# -*- coding: utf-8 -*-
import rich_click as click
from rich.panel import Panel

from kiara.kiara.config import KiaraGlobalConfig
from kiara.utils import rich_print
from kiara.utils.output import (
    create_table_from_base_model_cls,
    create_table_from_model_object,
)


@click.group("config")
@click.pass_context
def config(ctx):
    """Config-related sub-commands."""


@config.command("show")
@click.pass_context
def show(ctx):
    """Print the (current) kiara context configuration."""

    kgc: KiaraGlobalConfig = ctx.obj["kiara_global_config"]

    table = create_table_from_model_object(kgc)
    print()
    rich_print(Panel(table))


@config.command("help")
@click.pass_context
def config_help(ctx):
    """Print available configuration options and information about them."""

    table = create_table_from_base_model_cls(model_cls=KiaraGlobalConfig)
    print()
    rich_print(Panel(table))
