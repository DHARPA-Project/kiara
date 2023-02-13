# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""A command-line interface for *Kiara*.
"""

import logging
import sys
from typing import TYPE_CHECKING, Tuple, Union

import rich_click as click
import structlog
from rich.markdown import Markdown

from kiara.defaults import (
    SYMLINK_ISSUE_MSG,
)
from kiara.interfaces import KiaraAPIWrap
from kiara.utils import is_debug
from kiara.utils.class_loading import find_all_cli_subcommands
from kiara.utils.cli import terminal_print

if TYPE_CHECKING:
    pass

click.rich_click.USE_RICH_MARKUP = True


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
@click.pass_context
def cli(
    ctx,
    config: Union[str, None],
    context: Union[str, None],
    pipelines: Tuple[str],
    plugin: Union[str, None],
):
    """[i b]kiara[/b i] ia a data-orchestration framework; this is the command-line frontend for it.



    For more information, visit the [i][b]kiara[/b] homepage[/i]: https://dharpa.org/kiara.documentation .
    """

    # check if windows symlink work
    from kiara.utils.windows import check_symlink_works

    if not check_symlink_works():

        terminal_print()
        terminal_print(Markdown(SYMLINK_ISSUE_MSG))
        sys.exit(1)

    ctx.obj = {}

    lazy_wrapper = KiaraAPIWrap(config, context, pipelines, plugin)
    ctx.obj = lazy_wrapper
    # ctx.obj["kiara"] = api.context
    # ctx.obj["kiara_config"] = kiara_config
    # ctx.obj["kiara_context_name"] = context


for plugin in find_all_cli_subcommands():
    cli.add_command(plugin)

if __name__ == "__main__":
    cli()
