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
import typing

from kiara.context import Kiara
from kiara.context.config import KiaraGlobalConfig
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
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )


@click.group()
@click.option("--context", "-c", help="The kiara context to use.", required=False)
@click.option(
    "--pipeline-folder",
    "-p",
    help="Folder(s) that contain extra pipelines.",
    multiple=True,
    required=False,
)
@click.pass_context
def cli(ctx, context, pipeline_folder: typing.Tuple[str]):
    """Main cli entry-point, contains all the sub-commands."""

    ctx.obj = {}
    extra_config: typing.Dict[str, typing.Any] = {}
    if pipeline_folder:
        extra_config["extra_pipeline_folders"] = list(pipeline_folder)
    if context:
        extra_config["context"] = context
    extra_config["create_context"] = False
    kc = KiaraGlobalConfig(**extra_config)
    selected_config = kc.get_context()
    ctx.obj["kiara_global_config"] = kc
    ctx.obj["kiara"] = Kiara(config=selected_config)


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
