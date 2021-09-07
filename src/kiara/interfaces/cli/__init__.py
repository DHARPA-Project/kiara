# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click

from kiara import Kiara
from kiara.utils import is_develop

from .data.commands import data
from .explain import explain
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


cli.add_command(explain)
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

        print("HELLO")
        # import dearpygui.dearpygui as dpg
        #
        # from kiara import Kiara
        #
        # kiara = Kiara.instance()
        #
        # with dpg.window(width=300):
        #     dpg.add_listbox(kiara.available_module_types)
        #
        # dpg.start_dearpygui()

        # kiara: Kiara = ctx.obj["kiara"]

        # mod = kiara.create_module("logic.and", module_config={"constants": {"a": True}})
        # rich_print(mod.input_schemas)
        #
        # result = mod.run(b=True)
        # print(result.get_all_value_data())

        # and_pipeline = kiara.create_workflow("/home/markus/projects/dharpa/kiara/tests/resources/pipelines/logic/logic_4.json")
        # print_ascii_graph(and_pipeline.structure.data_flow_graph)
        #
        # and_pipeline.inputs.set_value("and_1_1__b", True)
        # and_pipeline.inputs.set_value("and_1_2__a", True)
        # and_pipeline.inputs.set_value("and_1_2__b", True)
        #
        # kiara.explain(and_pipeline.current_state)
        #
        # print(and_pipeline.outputs.get_all_value_data())
        #
        # # and_pipeline.inputs.set_value("b", False)
        # #
        # # print(and_pipeline.outputs.get_all_value_data())
        #
        # import pp
        # value_slot: ValueSlot = and_pipeline.outputs._value_slots['and_1_1__y']
        #
        # for k, v in value_slot.values.items():
        #     print("---")
        #     print(k)
        #     try:
        #         print(v.get_value_data())
        #     except Exception:
        #         print("-- not set --")
        #         pass


if __name__ == "__main__":
    cli()
