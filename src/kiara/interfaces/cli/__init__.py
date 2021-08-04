# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click

from kiara import Kiara
from kiara.utils import is_develop
from kiara.utils.output import rich_print

from .data.commands import data
from .info.commands import info
from .metadata.commands import metadata
from .module.commands import module
from .operation.commands import operation
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
cli.add_command(operation)
if is_develop():
    cli.add_command(info)

try:
    from .service.commands import service

    cli.add_command(service)
except ModuleNotFoundError:
    pass


@cli.command()
@click.pass_context
def dev(ctx):

    kiara = ctx.obj["kiara"]

    # from kiara.utils.global_metadata import get_metadata_for_python_module
    # md = get_metadata_for_python_module("kiara_modules.core.onboarding")
    # rich_print(md.json(indent=2))

    model_cls = kiara.get_module_class("array.load")
    md = model_cls.get_type_metadata()
    rich_print(md)


if __name__ == "__main__":
    cli()
