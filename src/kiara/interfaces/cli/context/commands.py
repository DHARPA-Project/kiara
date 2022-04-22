# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click
import sys

from kiara import Kiara
from kiara.registries.environment import EnvironmentRegistry
from kiara.utils.cli import output_format_option, terminal_print, terminal_print_model


@click.group("context")
@click.pass_context
def context(ctx):
    """Kiara context related sub-commands."""


# @context.command("create")
# @click.argument("context_name", nargs=1)
# @click.pass_context
# def create(ctx, context_name: str):
#
#     kiara_obj = Kiara.create(config=None, context_name=context_name)
#
#     terminal_print_model(
#         kiara_obj.context_info,
#         format=format,
#         in_panel=f"Context info for new kiara context: {context_name}",
#     )


@context.command("print")
@output_format_option()
@click.pass_context
def print_context(ctx, format: str):

    kiara_obj: Kiara = ctx.obj["kiara"]

    terminal_print_model(
        kiara_obj.context_info,
        format=format,
        in_panel=f"Context info for kiara id: {kiara_obj.id}",
    )


@context.group("config")
@click.pass_context
def config(ctx):
    """Config-related sub-commands."""


# @config.command("print")
# @output_format_option()
# @click.pass_context
# def print_config(ctx, format):
#     """Print the (current) kiara context configuration."""
#
#     kiara_obj: Kiara = ctx.obj["kiara"]
#
#     terminal_print_model(
#         kiara_obj.context_config,
#         format=format,
#         in_panel=f"kiara context config: [b i]{kiara_obj.context_config.context_alias}[/b i]",
#     )


# @config.command("help")
# @click.pass_context
# def config_help(ctx):
#     """Print available configuration options and information about them."""
#
#     table = create_table_from_base_model_cls(model_cls=KiaraCurrentContextConfig)
#     print()
#     terminal_print(Panel(table))


@context.group(name="environment")
@click.pass_context
def env_group(ctx):
    """Runtime environment-related sub-commands."""


@env_group.command("list")
@click.pass_context
def list_envs(ctx):
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


@context.group()
@click.pass_context
def metadata(ctx):
    """Metadata-related sub-commands."""


@metadata.command(name="list")
@output_format_option()
@click.pass_context
def list_metadata(ctx, format):
    """List available metadata schemas."""

    kiara_obj: Kiara = ctx.obj["kiara"]
    metadata_types = kiara_obj.context_info.metadata_types

    terminal_print_model(
        metadata_types, format=format, in_panel="Available metadata types"
    )


@metadata.command(name="explain")
@click.argument("metadata_key", nargs=1, required=True)
@click.option(
    "--details",
    "-d",
    help="Print more metadata schema details (for 'terminal' format).",
    is_flag=True,
)
@output_format_option()
@click.pass_context
def explain_metadata(ctx, metadata_key, format, details):
    """Print details for a specific metadata schema."""

    kiara_obj: Kiara = ctx.obj["kiara"]
    metadata_types = kiara_obj.context_info.metadata_types

    if metadata_key not in metadata_types.keys():
        print()
        print(f"No metadata schema for key '{metadata_key}' found...")
        sys.exit(1)

    info_obj = metadata_types[metadata_key]

    terminal_print_model(
        info_obj,
        format=format,
        in_panel=f"Details for metadata type: [b i]{metadata_key}[/b i]",
    )
