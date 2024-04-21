# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""A command-line interface for *Kiara*."""

import logging
import sys
from typing import Tuple, Union

import rich_click as click
import structlog

from kiara.interfaces import BaseAPIWrap
from kiara.utils import is_debug, log_message
from kiara.utils.class_loading import find_all_cli_subcommands
from kiara.utils.cli import (
    kiara_runtime_info_option,
    kiara_version_option,
    terminal_print,
)

click.rich_click.USE_RICH_MARKUP = True
# TODO: rich_click backport this
# click.rich_click._get_rich_console = get_console


if is_debug():
    logger = structlog.get_logger()

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    )
else:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
    )

CLICK_CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CLICK_CONTEXT_SETTINGS)
@click.option(
    "--config",
    "-cnf",
    help="A kiara config file (or folder containing one named 'kiara.config').",
    required=False,
)
@click.option(
    "--context",
    "-ctx",
    "-c",
    help="The name of the kiara context to use (or the path to a context file).",
    required=False,
)
@click.option(
    "--pipelines",
    "-p",
    help="File(s) and folder(s) that contain extra pipeline definitions.",
    multiple=True,
    required=False,
)
@click.option(
    "--plugin",
    "-P",
    help="Ensure the provided plugin package(s) are installed in the virtual environment.",
)
@click.option(
    "--use-background-service",
    "-b",
    is_flag=True,
    help="Always use the background service (start if not already running).",
    default=False,
)
@kiara_version_option()
@kiara_runtime_info_option()
@click.pass_context
def cli(
    ctx,
    config: Union[str, None],
    context: Union[str, None],
    pipelines: Tuple[str],
    plugin: Union[str, None],
    use_background_service: Union[bool, None] = None,
):
    """
    [i b]kiara[/b i] ia a data-orchestration framework; this is the command-line frontend for it.

    For more information, visit the [i][b]kiara[/b] homepage[/i]: https://dharpa.org/kiara.documentation .
    """
    # check if windows symlink work

    # if not check_symlink_works():
    #
    #     terminal_print()
    #     from rich.markdown import Markdown
    #
    #     terminal_print(Markdown(SYMLINK_ISSUE_MSG))
    #     sys.exit(1)

    context_subcommand = ctx.invoked_subcommand == "context"
    if context_subcommand and use_background_service:
        log_message(
            "ignore.background_service_request",
            reason="Not using background service for 'context' subcommand.",
        )

    if use_background_service and not context_subcommand:

        from kiara.zmq import (
            KiaraZmqServiceDetails,
            get_context_details,
            start_zmq_service,
        )
        from kiara.zmq.client import KiaraZmqClient

        if context is None:
            context = "default"

        context_details_data = get_context_details(context_name=context)
        if context_details_data is None:

            timeout = 120 * 1000  # 2 minutes default timeout
            api_wrap = BaseAPIWrap(config, context, pipelines, plugin)
            context_details = start_zmq_service(
                api_wrap=api_wrap,
                host=None,
                port=None,
                monitor=False,
                stdout=None,
                stderr=None,
                timeout=timeout,
            )
        else:
            context_details = KiaraZmqServiceDetails(**context_details_data)

        CONTEXT_ARG_NAMES = [
            "--config",
            "--context",
            "--pipelines",
            "--plugin",
            "-cnf",
            "-ctx",
            "-c",
            "-p",
            "-P",
        ]
        CONTEXT_FLAG_NAMES = ["--use-background-service", "-b"]

        arguments = []
        still_prefix = True
        for arg in sys.argv[1:]:
            if still_prefix and arg in CONTEXT_ARG_NAMES:
                # TODO: check arg with running service
                raise NotImplementedError(
                    "Custom context args for background service client not supported yet."
                )

            if still_prefix and arg in CONTEXT_FLAG_NAMES:
                continue
            else:
                still_prefix = False

            arguments.append(arg)

        assert context_details is not None

        zmq_client = KiaraZmqClient(
            host=context_details.host, port=context_details.port
        )

        try:
            zmq_client.request_cli(args=arguments)
            sys.exit(0)
        except KeyboardInterrupt:
            terminal_print("\nInterrupted by user, closing connection to service...")
            sys.exit(1)
        finally:
            zmq_client.close()

    else:
        lazy_wrapper = BaseAPIWrap(config, context, pipelines, plugin)
        ctx.obj = lazy_wrapper


for plugin in find_all_cli_subcommands():
    cli.add_command(plugin)

if __name__ == "__main__":
    cli()
