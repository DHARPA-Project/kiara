# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""A command-line interface for *Kiara*.
"""

import logging
import rich_click as click
import structlog
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from kiara.context.config import KiaraConfig
from kiara.defaults import KIARA_CONFIG_FILE_NAME, KIARA_MAIN_CONFIG_FILE
from kiara.utils import is_debug, is_develop
from kiara.utils.cli import terminal_print

from .context.commands import context
from .data.commands import data
from .dev.commands import dev_group
from .module.commands import module
from .operation.commands import operation
from .pipeline.commands import pipeline
from .run import run

# from .service.commands import service
from .type.commands import type_group

# click.rich_click.USE_MARKDOWN = True
click.rich_click.USE_RICH_MARKUP = True

# try:
#     import uvloop
#
#     uvloop.install()
# except Exception:
#     pass

# click.anyio_backend = "asyncio"

if is_debug():
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    )
else:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
    )


@click.group()
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
    "--pipeline-folder",
    "-p",
    help="Folder(s) that contain extra pipeline definitions.",
    multiple=True,
    required=False,
)
@click.pass_context
def cli(
    ctx, config: Optional[str], context: Optional[str], pipeline_folder: Tuple[str]
):
    """[i b]kiara[/b i] ia a data-orchestration framework, this is the command-line frontend for it.



    For more information, visit the [i][b]kiara[/b] homepage[/i]]: https://dharpa.org/kiara.documentation .
    """

    ctx.obj = {}
    extra_context_config: Dict[str, Any] = {}
    if pipeline_folder:
        extra_context_config["extra_pipeline_folders"] = list(pipeline_folder)

    extra_context_config["create_context"] = False

    kiara_config: Optional[KiaraConfig] = None
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
        context = kiara_config.default_context

    kiara = kiara_config.create_context(context=context)
    ctx.obj["kiara"] = kiara


# cli.add_command(explain)
cli.add_command(run)
cli.add_command(data)
cli.add_command(operation)
cli.add_command(module)
cli.add_command(pipeline)
cli.add_command(type_group)
cli.add_command(context)

# try:
#     pass
#
#     cli.add_command(service)
# except Exception:
#     log_message("skip.service_subcommand", reason="'fastapi' package not installed")

if is_develop():
    cli.add_command(dev_group)

if __name__ == "__main__":
    cli()
