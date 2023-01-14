# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click
import sys
from rich.panel import Panel
from typing import Tuple, Union

from kiara import KiaraAPI
from kiara.context import Kiara, KiaraConfig, KiaraContextConfig
from kiara.interfaces import get_console
from kiara.models.context import ContextInfo, ContextInfos
from kiara.models.values.value_metadata import ValueMetadata
from kiara.registries.environment import EnvironmentRegistry
from kiara.utils.cli import output_format_option, terminal_print, terminal_print_model
from kiara.utils.output import create_table_from_base_model_cls


@click.group("context")
@click.pass_context
def context(ctx):
    """Kiara context related sub-commands."""


@context.command("list")
@click.pass_context
def list_contexts(ctx) -> None:
    """List existing contexts."""

    kiara_api: KiaraAPI = ctx.obj["kiara_api"]

    summaries = kiara_api.retrieve_context_infos()

    terminal_print(summaries)


@context.command("explain")
@click.argument("context_name", nargs=-1, required=False)
@click.option("--value-ids", "-i", help="Show value ids.", is_flag=True, default=False)
@output_format_option()
@click.pass_context
def explain_context(
    ctx, format: str, value_ids: bool, context_name: Union[Tuple[str], None] = None
):
    """Print details for one or several contexts."""

    kiara_config: KiaraConfig = ctx.obj["kiara_config"]

    if not context_name:
        cn = ctx.obj["kiara_context_name"]
        contexts = [cn]
    else:
        contexts = list(context_name)

    if len(contexts) == 1:

        kcc = kiara_config.get_context_config(contexts[0])
        cs = ContextInfo.create_from_context_config(
            kcc, context_name=contexts[0], runtime_config=kiara_config.runtime_config
        )
        terminal_print_model(
            cs, format=format, full_details=True, show_value_ids=value_ids
        )

    else:
        summaries = []
        for c in contexts:
            cc = kiara_config.get_context_config(c)
            cs = ContextInfo.create_from_context_config(
                cc, context_name=c, runtime_config=kiara_config.runtime_config
            )
            summaries.append(cs)
        terminal_print_model(
            *summaries, format=format, full_details=True, show_value_ids=value_ids
        )


@context.command("delete")
@click.argument("context_name", nargs=1, required=False)
@click.option(
    "--force", "-f", help="Delete without prompt.", is_flag=True, default=False
)
@click.option(
    "--all-contexts", "-a", help="Delete all contexts.", is_flag=True, default=False
)
@click.pass_context
def delete_context(
    ctx,
    context_name: Union[str, None] = None,
    force: bool = False,
    all_contexts: bool = False,
):
    """Delete a context and all its stored values."""

    kiara_config: KiaraConfig = ctx.obj["kiara_config"]

    if not context_name:
        if all_contexts:
            _context_name = "ALL_CONTEXTS"
        else:
            _context_name = ctx.obj["kiara_context_name"]
    else:
        if all_contexts:
            if context_name != "ALL_CONTEXTS":
                terminal_print()
                terminal_print(
                    f"Context name '{context_name}' specified, as well as '--all-contexts', this is not valid."
                )
                sys.exit(1)
        _context_name = context_name

    confirmed = False

    if _context_name == "ALL_CONTEXTS":
        if not force:
            summaries = ContextInfos.create_context_infos(
                contexts=kiara_config.context_configs
            )
            terminal_print_model(summaries, in_panel="All contexts:")
            user_input = get_console().input(
                f"Deleting all contexts, are you sure? \[yes/no]: "  # noqa
            )

            if user_input.lower() == "yes":
                confirmed = True
        else:
            confirmed = True

        if not confirmed:
            terminal_print("\nDoing nothing...")
            sys.exit(0)

        terminal_print("Deleting contexts...")
        for _context_name in kiara_config.context_configs.keys():
            terminal_print(f"  - {_context_name}")
            kiara_config.delete(context_name=_context_name, dry_run=False)

        terminal_print("Done.")

    else:

        if not force:

            context_summary = kiara_config.delete(
                context_name=_context_name, dry_run=True
            )
            terminal_print_model(
                context_summary,
                full_details=True,
                in_panel=f"Context details: {_context_name}",
            )
            terminal_print()
            user_input = get_console().input(
                f"Deleting context '[b i]{_context_name}[/b i]', are you sure? \[yes/no]: "  # noqa
            )

            if user_input.lower() == "yes":
                confirmed = True
        else:
            confirmed = True

        if not confirmed:
            terminal_print("\nDoing nothing...")
            sys.exit(0)

        terminal_print("Deleting context...")
        kiara_config.delete(context_name=_context_name, dry_run=False)

        terminal_print("Done.")


@context.group("config")
@click.pass_context
def config(ctx):
    """Config-related sub-commands."""


@config.command("print")
@output_format_option()
@click.pass_context
def print_config(ctx, format) -> None:
    """Print the (current) kiara context configuration."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    terminal_print_model(
        kiara_obj.context_config,
        format=format,
        in_panel=f"kiara context config: [b i]{kiara_obj.context_config.context_id}[/b i]",
    )


@config.command("help")
@click.pass_context
def config_help(ctx):
    """Print available configuration options and information about them."""

    table = create_table_from_base_model_cls(model_cls=KiaraContextConfig)
    print()
    terminal_print(Panel(table))


@context.group(name="runtime-info")
@click.pass_context
def runtime(ctx):
    """Information about runtime models, etc."""


@runtime.command("print")
@output_format_option()
@click.pass_context
def print_context(ctx, format: str):
    """Print all relevant models within the current runtime environment."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    terminal_print_model(
        kiara_obj.context_info,
        format=format,
        in_panel=f"Context info for kiara id: {kiara_obj.id}",
    )


@context.group(name="environment")
@click.pass_context
def env_group(ctx):
    """Environment-related sub-commands."""


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
def explain_env(ctx, env_type: str, format: str) -> None:

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
def list_metadata(ctx, format) -> None:
    """List available metadata schemas."""

    kiara_obj: Kiara = ctx.obj["kiara"]
    metadata_types = kiara_obj.kiara_model_registry.get_models_of_type(ValueMetadata)

    terminal_print_model(
        metadata_types, format=format, in_panel="Available metadata types"
    )


@metadata.command(name="explain")
@click.argument("metadata_key", nargs=1, required=True)
# @click.option(
#     "--details",
#     "-d",
#     help="Print more metadata schema details (for 'terminal' format).",
#     is_flag=True,
# )
@output_format_option()
@click.pass_context
def explain_metadata(ctx, metadata_key, format) -> None:
    """Print details for a specific metadata schema."""

    kiara_obj: Kiara = ctx.obj["kiara"]
    metadata_types = kiara_obj.kiara_model_registry.get_models_of_type(ValueMetadata)

    if metadata_key not in metadata_types.item_infos.keys():
        print()
        print(f"No metadata schema for key '{metadata_key}' found...")
        sys.exit(1)

    info_obj = metadata_types.item_infos[metadata_key]

    terminal_print_model(
        info_obj,
        format=format,
        in_panel=f"Details for metadata type: [b i]{metadata_key}[/b i]",
    )
