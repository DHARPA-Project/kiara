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
from typing import Any, Dict, Optional, Tuple

from kiara.context import Kiara
from kiara.context.config import KiaraConfig
from kiara.utils import is_debug, is_develop, log_message

from .context.commands import context
from .data.commands import data
from .dev.commands import dev_group
from .module.commands import module
from .operation.commands import operation
from .pipeline.commands import pipeline
from .run import run
from .service.commands import service
from .type.commands import type_group

click.rich_click.USE_MARKDOWN = True
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
@click.option("--config", "-c", help="A kiara config or context file.", required=False)
@click.option(
    "--context-name", "-ctx", help="The kiara context to use.", required=False
)
@click.option(
    "--pipeline-folder",
    "-p",
    help="Folder(s) that contain extra pipelines.",
    multiple=True,
    required=False,
)
@click.pass_context
def cli(
    ctx, config: Optional[str], context_name: Optional[str], pipeline_folder: Tuple[str]
):
    """Main cli entry-point, contains all the sub-commands."""

    ctx.obj = {}
    extra_context_config: Dict[str, Any] = {}
    if pipeline_folder:
        extra_context_config["extra_pipeline_folders"] = list(pipeline_folder)

    extra_context_config["create_context"] = False

    kiara: Optional[Kiara] = None
    if config:
        raise NotImplementedError()
        # if config.endswith("kiara_context.yaml"):
        #     kcc = KiaraCurrentContextConfig.load_context(config)
        #     kiara = Kiara(config=kcc)

    if kiara is None:
        kiara_config = KiaraConfig()

        kiara = kiara_config.create_context(context=context_name)

    ctx.obj["kiara"] = kiara


# cli.add_command(explain)
cli.add_command(run)
cli.add_command(data)
cli.add_command(operation)
cli.add_command(module)
cli.add_command(pipeline)
cli.add_command(type_group)
cli.add_command(context)

try:
    pass

    cli.add_command(service)
except Exception:
    log_message("skip.service_subcommand", reason="'fastapi' package not installed")

if is_develop():
    cli.add_command(dev_group)

if __name__ == "__main__":
    cli()
