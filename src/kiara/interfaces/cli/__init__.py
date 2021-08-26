# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click

from kiara import Kiara
from kiara.utils import is_develop

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
    ctx.obj["kiara"] = Kiara()


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

if is_develop():

    @cli.command()
    @click.pass_context
    def dev(ctx):

        kiara: Kiara = ctx.obj["kiara"]

        and_pipeline = kiara.create_workflow("logic.and")
        # print_ascii_graph(and_pipeline.structure.data_flow_graph)

        and_pipeline.inputs.set_value("a", True)
        # kiara.explain(and_pipeline.current_state)
        and_pipeline.inputs.set_value("b", True)
        kiara.explain(and_pipeline.current_state)

        # print(and_pipeline.outputs.get_all_value_data())
        #
        # and_pipeline.inputs.set_value("b", False)
        #
        # print(and_pipeline.outputs.get_all_value_data())
        #
        # value_slot: ValueSlot = and_pipeline.outputs._value_slots['y']
        #
        # for k, v in value_slot.values.items():
        #     try:
        #         print(v.get_value_data())
        #     except Exception:
        #         print("-- not set --")
        #         pass


if __name__ == "__main__":
    cli()
