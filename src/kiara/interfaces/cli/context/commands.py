# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import sys
from typing import TYPE_CHECKING, Tuple, Union

import rich_click as click
from rich import box
from rich.panel import Panel
from rich.table import Table

from kiara.interfaces import BaseAPIWrap, get_console
from kiara.utils import log_exception
from kiara.utils.cli import (
    dict_from_cli_args,
    output_format_option,
    terminal_print,
    terminal_print_model,
)

if TYPE_CHECKING:
    from kiara.interfaces.python_api.base_api import BaseAPI, Kiara, KiaraConfig


@click.group("context")
@click.pass_context
def context(ctx):
    """Kiara context related sub-commands."""


@context.command("list")
@click.pass_context
def list_contexts(ctx) -> None:
    """List existing contexts."""
    kiara_api: BaseAPI = ctx.obj.base_api

    summaries = kiara_api.retrieve_context_infos()

    terminal_print(summaries)


@context.command("explain")
@click.argument("context_name", nargs=-1, required=False)
@click.option("--value-ids", "-i", help="Show value ids.", is_flag=True, default=False)
@click.option(
    "--show-config", "-c", help="Also show kiara config.", is_flag=True, default=False
)
@output_format_option()
@click.pass_context
def explain_context(
    ctx,
    format: str,
    value_ids: bool,
    context_name: Union[Tuple[str], None] = None,
    show_config: bool = False,
):
    """Print details for one or several contexts."""
    kiara_config: KiaraConfig = ctx.obj.kiara_config

    if not context_name:
        cn = ctx.obj.kiara_context_name
        contexts = [cn]
    else:
        contexts = list(context_name)

    from kiara.models.context import ContextInfo

    render_config = {
        "show_lines": False,
        "show_header": False,
        "show_description": False,
    }

    if show_config:
        from rich.table import Table

        config = kiara_config.create_renderable(**render_config)
        table = Table(show_header=False, show_lines=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")
        if kiara_config._config_path:
            table.add_row("config file", f"  {kiara_config._config_path}")
        table.add_row("config", config)
        terminal_print(table, in_panel="Kiara config")

    if len(contexts) == 1:

        kcc = kiara_config.get_context_config(contexts[0])
        cs = ContextInfo.create_from_context_config(
            kcc, context_name=contexts[0], runtime_config=kiara_config.runtime_config
        )
        terminal_print_model(
            cs,
            format=format,
            full_details=True,
            show_value_ids=value_ids,
            in_panel=f"Context '{contexts[0]}'",
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
@click.option("--all", "-a", help="Delete all contexts.", is_flag=True, default=False)
@click.pass_context
def delete_context(
    ctx,
    context_name: Union[str, None] = None,
    force: bool = False,
    all: bool = False,
):
    """Delete a context and all its stored values."""
    kiara_config: KiaraConfig = ctx.obj.kiara_config

    if not context_name:
        if all:
            _context_name = "ALL_CONTEXTS"
        else:
            _context_name = ctx.obj.kiara_context_name
    else:
        if all:
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
            from kiara.models.context import ContextInfos

            summaries = ContextInfos.create_context_infos(
                contexts=kiara_config.context_configs
            )
            terminal_print_model(summaries, in_panel="All contexts:")
            user_input = get_console().input(
                r"Deleting all contexts, are you sure? \[yes/no]: "
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
            terminal_print()
            if not context_summary:
                terminal_print(
                    f"Can't determine context details for context '{_context_name}'."
                )
            else:
                terminal_print_model(
                    context_summary,
                    full_details=True,
                    in_panel=f"Context details: {_context_name}",
                )
            terminal_print()
            txt_pre = r"Deleting context '[b i]"
            txt_post = r"[/b i]', are you sure? \[yes/no]: "
            txt_all = f"{txt_pre}{_context_name}{txt_post}"
            user_input = get_console().input(
                txt_all,
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


@context.group("info")
@click.pass_context
def info(ctx):
    """Information about several aspects of the current/specified kiara context."""


@info.group("config")
@click.pass_context
def config(ctx):
    """Information about the current context configuration."""


@config.command("print")
@output_format_option()
@click.pass_context
def print_config(ctx, format) -> None:
    """Print the (current) kiara context configuration."""
    kiara_obj: Kiara = ctx.obj.kiara

    terminal_print_model(
        kiara_obj.context_config,
        format=format,
        in_panel=f"kiara context config: [b i]{kiara_obj.context_config.context_id}[/b i]",
    )


@config.command("help")
@click.pass_context
def config_help(ctx):
    """Print available configuration options and information about them."""
    from kiara.context import KiaraContextConfig
    from kiara.utils.output import create_table_from_base_model_cls

    table = create_table_from_base_model_cls(model_cls=KiaraContextConfig)
    terminal_print()
    terminal_print(Panel(table))


@info.group(name="environment")
@click.pass_context
def env_group(ctx):
    """Information about whats in the current environment details (Python virtual environment, available metadata models, etc)."""


@env_group.command("print")
@output_format_option()
@click.pass_context
def print_context(ctx, format: str):
    """Print all relevant models within the current (Python virtual) environment."""
    kiara_obj: Kiara = ctx.obj.kiara

    terminal_print_model(
        kiara_obj.context_info,
        format=format,
        in_panel=f"Context info for kiara id: {kiara_obj.id}",
    )


# @info.group(name="runtime")
# @click.pass_context
# def runtime_group(ctx):
#     """Runtime-related sub-commands."""
#
# @runtime_group.command("print")
# @output_format_option()
# @click.pass_context
# def print_runtime(ctx, format: str):
#     """Print runtime information."""
#     kiara_obj: Kiara = ctx.obj.kiara
#
#     terminal_print_model(
#         kiara_obj.runtime_config,
#         format=format,
#         in_panel=f"Runtime info for kiara id: {kiara_obj.id}",
#     )


@env_group.command("list")
@click.pass_context
def list_envs(ctx):
    """List available runtime environment information."""
    from kiara.registries.environment import EnvironmentRegistry

    env_reg = EnvironmentRegistry.instance()

    terminal_print(env_reg)


@env_group.command("explain")
@click.argument("env_type", metavar="ENVIRONMENT_TYPE", nargs=1, required=True)
@output_format_option()
@click.pass_context
def explain_env(ctx, env_type: str, format: str) -> None:

    from kiara.registries.environment import EnvironmentRegistry

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


@env_group.group()
@click.pass_context
def metadata(ctx):
    """Metadata-related sub-commands."""


@metadata.command(name="list")
@output_format_option()
@click.pass_context
def list_metadata(ctx, format) -> None:
    """List available metadata schemas."""
    kiara_obj: Kiara = ctx.obj.kiara
    from kiara.models.values.value_metadata import ValueMetadata

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
    kiara_obj: Kiara = ctx.obj.kiara
    from kiara.models.values.value_metadata import ValueMetadata

    metadata_types = kiara_obj.kiara_model_registry.get_models_of_type(ValueMetadata)

    if metadata_key not in metadata_types.item_infos.keys():
        terminal_print()
        terminal_print(f"No metadata schema for key '{metadata_key}' found...")
        sys.exit(1)

    info_obj = metadata_types.item_infos[metadata_key]

    terminal_print_model(
        info_obj,
        format=format,
        in_panel=f"Details for metadata type: [b i]{metadata_key}[/b i]",
    )


# @env_group.group()
# @click.pass_context
# def api(ctx):
#     """API-related sub-commands."""
#
#
# @api.command()
# @click.argument("filter", nargs=-1, required=False)
# @click.option(
#     "--full-doc",
#     "-d",
#     is_flag=True,
#     help="Display the full doc for all operations (when using 'terminal' as format).",
# )
# @click.pass_context
# def list_endpoints(ctx, filter, full_doc):
#     """List all available API endpoints."""
#     from kiara.interfaces.python_api import KiaraAPI
#     from kiara.interfaces.python_api.proxy import ApiEndpoints
#
#     exclude = ["get_runtime_config", "retrieve_workflow_info"]
#     endpoints = ApiEndpoints(api_cls=KiaraAPI, filters=filter, exclude=exclude)
#
#     terminal_print()
#     terminal_print(endpoints, in_panel="API endpoints", full_doc=full_doc)
#
#
@context.group("service")
@click.pass_context
def service(ctx):
    """Service-related sub-commands."""


@service.command("start")
@click.option("--host", help="The host to bind to.", default="localhost")
@click.option("--port", help="The port to bind to.", required=False, default=0)
@click.option(
    "--monitor",
    help="Monitor the service.",
    required=False,
    default=False,
    is_flag=True,
)
@click.option(
    "--stdout",
    "-o",
    help="Write output logs to this file.",
    required=False,
    default=None,
)
@click.option(
    "--stderr",
    "-e",
    help="Write error logs to this file.",
    required=False,
    default=None,
)
@click.option(
    "--timeout",
    "-t",
    help="If set, the service shuts down if not request happens within the specified timeout (in milliseconds).",
    required=False,
    default=0,
)
@click.pass_context
def start_service(
    ctx,
    host: str,
    port: int,
    monitor: bool = False,
    stdout: Union[str, None] = None,
    stderr: Union[str, None] = None,
    timeout: int = 0,
):
    """Start a kiara zmq service for this context."""

    from kiara.utils.output import create_table_from_model_object
    from kiara.zmq import start_zmq_service

    api_wrap: BaseAPIWrap = ctx.obj

    try:
        details = start_zmq_service(
            api_wrap=api_wrap,
            host=host,
            port=port,
            monitor=monitor,
            stdout=stdout,
            stderr=stderr,
            timeout=timeout,
        )

        if not monitor:
            assert details is not None
            if not details.newly_started:
                terminal_print()
                terminal_print(
                    f"Service for context '{api_wrap.kiara_context_name}' already running, doing nothing..."
                )
            else:
                terminal_print()
                terminal_print("Started service in background process:")
                terminal_print()
                table = create_table_from_model_object(
                    details,
                    render_config={"show_header": False, "show_type_column": False},
                )
                terminal_print(table, in_panel="Service details")

    except Exception as e:
        import traceback

        traceback.print_exc()
        log_exception(e)
        terminal_print()
        terminal_print(f"Error starting service: {e}")
        sys.exit(1)


@service.command("stop")
@click.argument("context_name", nargs=1, required=False)
@click.pass_context
def stop_service(ctx, context_name: Union[None, str]):
    """Stop a running zmq service for this context."""

    from kiara.zmq import get_context_details
    from kiara.zmq.client import KiaraZmqClient

    if not context_name:
        context_name = ctx.obj.kiara_context_name

    context_details = get_context_details(context_name=context_name)  # type: ignore
    if not context_details:
        terminal_print()
        terminal_print(
            f"No service running for context '{context_name}'. Doing nothing..."
        )
        sys.exit(0)

    host = context_details["host"]
    port = context_details["port"]
    zmq_client = KiaraZmqClient(host=host, port=port)
    zmq_client.request(endpoint_name="stop", args={})

    terminal_print()
    terminal_print(f"Stopped context: {context_name}")

    sys.exit(0)


@service.command("list")
@click.option("--details", "-d", help="Show more details.", is_flag=True, default=False)
@click.pass_context
def list_services(ctx, details: bool):
    """List all contexts that have a currently running service."""

    from kiara.zmq import get_context_details, list_registered_contexts
    from kiara.zmq.client import KiaraZmqClient

    contexts = list_registered_contexts()
    if not contexts:
        terminal_print()
        terminal_print("No services running.")
        sys.exit(0)

    terminal_print()

    if not details:
        terminal_print("Running services:")
        for c in contexts:
            terminal_print(f" - {c}")

    else:

        from kiara.context import KiaraContextConfig, KiaraRuntimeConfig
        from kiara.utils.output import create_table_from_model_object

        table = Table(show_header=True, show_lines=False, box=box.SIMPLE)
        table.add_column("context")
        table.add_column("details")

        for c in contexts:
            context_details = get_context_details(c)
            if not context_details:
                table.add_row(c, "Can't get context details, skipping...")
                continue
            zmq_client = KiaraZmqClient(
                host=context_details["host"], port=context_details["port"]
            )
            status = zmq_client.request(endpoint_name="service_status", args={})
            state = status["state"]
            timeout = status["timeout"]
            context_config = KiaraContextConfig(**status["context_config"])
            runtime_config = KiaraRuntimeConfig(**status["runtime_config"])

            config_table = create_table_from_model_object(context_config)
            runtime_table = create_table_from_model_object(runtime_config)

            c_table = Table(show_header=False, show_lines=False, box=box.SIMPLE)
            c_table.add_column("key", style="i")
            c_table.add_column("value")
            c_table.add_row("state", state)
            c_table.add_row("timeout", str(timeout))
            c_table.add_row("context config", config_table)
            c_table.add_row("runtime config", runtime_table)

            table.add_row(c, c_table)

        terminal_print(table, in_panel="Service details")


@service.command("request")
@click.option(
    "--context", "-c", help="The context to use.", required=False, default=None
)
@click.argument("endpoint", required=True, nargs=1)
@click.argument("arguments", required=False, nargs=-1)
@click.pass_context
def request(
    ctx, endpoint: str, arguments: Tuple[str], context: Union[None, str] = None
):
    """Send a request to a kiara zmq service."""

    from kiara.zmq import get_context_details
    from kiara.zmq.client import KiaraZmqClient

    if not context:
        context = ctx.obj.kiara_context_name

    context_details = get_context_details(context_name=context)  # type: ignore
    if not context_details:
        terminal_print()
        terminal_print(f"No service running for context '{context}'. Doing nothing...")
        sys.exit(1)

    zmq_client = KiaraZmqClient(
        host=context_details["host"], port=context_details["port"]
    )

    args = dict_from_cli_args(*arguments)

    try:
        result = zmq_client.request(endpoint_name=endpoint, args=args)
        print(result)  # noqa
    except KeyboardInterrupt:
        terminal_print("\nInterrupted by user, closing connection to service...")
    finally:
        zmq_client.close()


CLI_CLIENT_CLICK_CONTEXT_SETTINGS = {
    "help_option_names": [],
    "ignore_unknown_options": True,
}


@service.command("request-cli", context_settings=CLI_CLIENT_CLICK_CONTEXT_SETTINGS)
@click.option(
    "--context", "-c", help="The context to use.", required=False, default=None
)
@click.argument("arguments", required=False, nargs=-1)
@click.pass_context
def request_cli(ctx, arguments: Tuple[str], context: Union[None, str] = None):
    """Send a request to a kiara zmq service."""
    from kiara.zmq import get_context_details
    from kiara.zmq.client import KiaraZmqClient

    if not context:
        context = ctx.obj.kiara_context_name

    context_details = get_context_details(context_name=context)  # type: ignore
    if not context_details:
        terminal_print()
        terminal_print(f"No service running for context '{context}'. Doing nothing...")
        sys.exit(1)

    zmq_client = KiaraZmqClient(
        host=context_details["host"], port=context_details["port"]
    )

    try:
        zmq_client.request_cli(args=arguments)
    except KeyboardInterrupt:
        terminal_print("\nInterrupted by user, closing connection to service...")
    finally:
        zmq_client.close()
