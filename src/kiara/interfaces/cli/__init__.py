# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import click
import typing

from kiara import Kiara
from kiara.config import KiaraConfig
from kiara.utils import is_develop

from .data.commands import data
from .dev.commands import dev_group
from .explain import explain
from .info.commands import info
from .metadata.commands import metadata
from .module.commands import module
from .operation.commands import operation
from .pipeline.commands import pipeline
from .run import run
from .type.commands import type_group

# try:
#     import uvloop
#
#     uvloop.install()
# except Exception:
#     pass

# click.anyio_backend = "asyncio"


@click.group()
@click.option(
    "--pipeline-folder",
    "-p",
    help="Folder(s) that contain extra pipelines.",
    multiple=True,
    required=False,
)
@click.pass_context
def cli(ctx, pipeline_folder: typing.Tuple[str]):
    """Main cli entry-point, contains all the sub-commands."""

    ctx.obj = {}
    kc = KiaraConfig()
    if pipeline_folder:
        kc.extra_pipeline_folders = list(pipeline_folder)

    ctx.obj["kiara"] = Kiara(config=kc)


cli.add_command(explain)
cli.add_command(run)
cli.add_command(data)
cli.add_command(operation)
cli.add_command(module)
cli.add_command(pipeline)
cli.add_command(info)
cli.add_command(metadata)
cli.add_command(type_group)
if is_develop():
    cli.add_command(dev_group)

if __name__ == "__main__":
    cli()
