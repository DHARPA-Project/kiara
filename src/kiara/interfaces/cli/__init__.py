# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click

from kiara import Kiara

from .data.commands import data
from .metadata.commands import metadata
from .module.commands import module
from .pipeline.commands import pipeline
from .run import run
from .type.command import type_group

try:
    import uvloop

    uvloop.install()
except Exception:
    pass

click.anyio_backend = "asyncio"


@click.group()
@click.pass_context
def cli(ctx):
    """Main cli entry-point, contains all the sub-commands."""

    ctx.obj = {}
    ctx.obj["kiara"] = Kiara.instance()


cli.add_command(run)
cli.add_command(data)
cli.add_command(metadata)
cli.add_command(type_group)
cli.add_command(module)
cli.add_command(pipeline)


if __name__ == "__main__":
    cli()
