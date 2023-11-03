# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""A command-line interface for *Kiara*."""

import logging

import rich_click as click
import structlog

from kiara.utils import is_debug
from kiara.utils.class_loading import find_all_cli_subcommands

click.rich_click.USE_RICH_MARKUP = True

# TODO: rich_click refactoring, how to backport this?
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
@click.pass_context
def proxy_cli(
    ctx,
):

    assert ctx.obj is not None


for plugin in find_all_cli_subcommands():
    proxy_cli.add_command(plugin)
