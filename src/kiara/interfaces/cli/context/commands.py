# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click
import sys
from rich.panel import Panel

from kiara.kiara.config import KiaraGlobalConfig
from kiara.registries.environment import EnvironmentRegistry
from kiara.utils.cli import output_format_option, terminal_print, terminal_print_model
from kiara.utils.output import create_table_from_base_model_cls


@click.group("context")
@click.pass_context
def context(ctx):
    """Kiara context related sub-commands."""


@context.group("config")
@click.pass_context
def config(ctx):
    """Config-related sub-commands."""


@config.command("show")
@output_format_option()
@click.pass_context
def show(ctx, format):
    """Print the (current) kiara context configuration."""

    kgc: KiaraGlobalConfig = ctx.obj["kiara_global_config"]

    terminal_print_model(
        kgc, format=format, in_panel=f"kiara context config: [b i]{kgc.context}[/b i]"
    )


@config.command("help")
@click.pass_context
def config_help(ctx):
    """Print available configuration options and information about them."""

    table = create_table_from_base_model_cls(model_cls=KiaraGlobalConfig)
    print()
    terminal_print(Panel(table))


@context.group(name="environment")
@click.pass_context
def env_group(ctx):
    """Runtime environment-related sub-commands."""


@env_group.command()
@click.pass_context
def list(ctx):
    """List available runtime environment information."""

    env_reg = EnvironmentRegistry.instance()

    terminal_print(env_reg)


@env_group.command("explain")
@click.argument("env_type", metavar="ENVIRONMENT_TYPE", nargs=1, required=True)
@output_format_option()
@click.pass_context
def explain_env(ctx, env_type: str, format: str):

    env_reg = EnvironmentRegistry.instance()

    env = env_reg.environments.get(env_type, None)
    if env is None:
        terminal_print()
        terminal_print(
            f"No environment with name '{env_type}' available. Available types: {', '.join(env_reg.environments.keys())}"
        )
        sys.exit()

    terminal_print_model(
        env,
        format=format,
        in_panel=f"Details for environment: [b i]{env_type}[/b i]",
        summary=False,
    )
