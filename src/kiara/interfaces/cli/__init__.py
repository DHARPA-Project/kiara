# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""A command-line interface for *Kiara*.
"""

import logging
import os
import rich_click as click
import structlog
import sys
from pathlib import Path
from rich.markdown import Markdown
from typing import Tuple, Union

from kiara.context.config import KiaraConfig
from kiara.defaults import (
    KIARA_CONFIG_FILE_NAME,
    KIARA_MAIN_CONFIG_FILE,
    SYMLINK_ISSUE_MSG,
)
from kiara.interfaces.python_api import KiaraAPI
from kiara.utils import is_debug
from kiara.utils.class_loading import find_all_cli_subcommands
from kiara.utils.cli import terminal_print

# from .service.commands import service

# click.rich_click.USE_MARKDOWN = True
click.rich_click.USE_RICH_MARKUP = True


if is_debug():
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    )
else:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
    )

CLICK_CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


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
@click.pass_context
def cli(
    ctx, config: Union[str, None], context: Union[str, None], pipelines: Tuple[str]
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

    # kiara_config: Optional[KiaraConfig] = None
    exists = False
    create = False
    if config:
        config_path = Path(config)
        if config_path.exists():
            if config_path.is_file():
                config_file_path = config_path
                exists = True
            else:
                config_file_path = config_path / KIARA_CONFIG_FILE_NAME
                if config_file_path.exists():
                    exists = True

    else:
        config_file_path = Path(KIARA_MAIN_CONFIG_FILE)
        if not config_file_path.exists():
            create = True
            exists = False
        else:
            exists = True

    if not exists:
        if not create:
            terminal_print()
            terminal_print(
                f"Can't create kiara context, specified config file does not exist: {config}."
            )
            sys.exit(1)

        kiara_config = KiaraConfig()
        kiara_config.save(config_file_path)
    else:
        kiara_config = KiaraConfig.load_from_file(config_file_path)

    ctx.obj["kiara_config"] = kiara_config

    if not context:
        context = os.environ.get("KIARA_CONTEXT", None)

    if not context:
        context = kiara_config.default_context

    kiara = kiara_config.create_context(context=context, extra_pipelines=pipelines)
    ctx.obj["kiara"] = kiara
    ctx.obj["kiara_api"] = KiaraAPI(kiara=kiara)
    ctx.obj["kiara_config"] = kiara_config
    ctx.obj["kiara_context_name"] = context


for plugin in find_all_cli_subcommands():
    cli.add_command(plugin)

if __name__ == "__main__":
    cli()
