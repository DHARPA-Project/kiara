# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click
import os.path
import sys
import typing
from rich import print as rich_print

from kiara import Kiara
from kiara.module import ModuleInfo
from kiara.pipeline.controller import BatchController
from kiara.pipeline.module import PipelineModuleInfo
from kiara.utils import module_config_from_cli_args

# from importlib.metadata import entry_points


# from asciinet import graph_to_ascii


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


@cli.group()
@click.pass_context
def module(ctx):
    """Information about available modules, and details about them."""


@module.command(name="list")
@click.option(
    "--only-pipeline-modules", "-p", is_flag=True, help="Only list pipeline modules."
)
@click.option(
    "--only-core-modules",
    "-c",
    is_flag=True,
    help="Only list core (aka 'Python') modules.",
)
@click.pass_context
def list_modules(ctx, only_pipeline_modules: bool, only_core_modules: bool):
    """List available (Python) module types."""

    if only_pipeline_modules and only_core_modules:
        rich_print()
        rich_print(
            "Please provide either '--only-core-modules' or '--only-pipeline-modules', not both."
        )
        sys.exit(1)

    kiara_obj = ctx.obj["kiara"]

    if only_pipeline_modules:
        m_list = kiara_obj.available_pipeline_module_types
    elif only_core_modules:
        m_list = kiara_obj.available_non_pipeline_module_types
    else:
        m_list = kiara_obj.available_module_types

    for name in m_list:
        rich_print(name)


@module.command(name="describe-type")
@click.argument("module_type", nargs=1, required=True)
@click.pass_context
def describe_module_type(ctx, module_type: str):
    """Print details of a (Python) module."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    if os.path.isfile(module_type):
        _module_type: str = kiara_obj.register_pipeline_description(  # type: ignore
            module_type, raise_exception=True
        )  # type: ignore
    else:
        _module_type = module_type

    m_cls = kiara_obj.get_module_class(_module_type)
    if _module_type == "pipeline" or not m_cls.is_pipeline():
        info = ModuleInfo(module_type=_module_type, _kiara=kiara_obj)
    else:
        info = ModuleInfo(module_type=_module_type, _kiara=kiara_obj)
    rich_print()
    rich_print(info)


@module.command("describe")
@click.option("--module-type", "-t", required=True)
@click.option(
    "--config",
    "-c",
    multiple=True,
    required=False,
    help="Configuration values for module initialization.",
)
@click.pass_context
def describe_module(ctx, module_type: str, config: typing.Iterable[typing.Any]):
    """Describe a step.

    A step, in this context, is basically a an instantiated module class, incl. (optional) config."""

    config = module_config_from_cli_args(*config)

    kiara_obj = ctx.obj["kiara"]
    if os.path.isfile(module_type):
        module_type = kiara_obj.register_pipeline_description(
            module_type, raise_exception=True
        )

    module_obj = kiara_obj.create_module(
        id=module_type, module_type=module_type, module_config=config
    )
    rich_print()
    rich_print(module_obj)


@cli.group()
@click.pass_context
def pipeline(ctx):
    """Pipeline-related sub-commands."""


@pipeline.command()
@click.option("--pipeline-type", "-t", required=True)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help="Display full data-flow graph, incl. intermediate input/output connections.",
)
@click.option(
    "--config",
    "-c",
    multiple=True,
    required=False,
    help="Configuration values for module initialization.",
)
@click.pass_context
def data_flow_graph(
    ctx, pipeline_type: str, config: typing.Iterable[typing.Any], full: bool
):
    """Print the data flow graph for a pipeline structure."""

    config = module_config_from_cli_args(*config)
    if config:
        print("Pipeline config not supported yet.")
        sys.exit()

    kiara_obj = ctx.obj["kiara"]
    if os.path.isfile(pipeline_type):
        pipeline_type = kiara_obj.register_pipeline_description(
            pipeline_type, raise_exception=True
        )

    m_cls = kiara_obj.get_module_class(pipeline_type)
    if not m_cls.is_pipeline():
        rich_print()
        rich_print(f"Module '{pipeline_type}' is not a pipeline-type module.")
        sys.exit(1)

    info = PipelineModuleInfo(module_type=pipeline_type)

    info.print_data_flow_graph(simplified=not full)


@pipeline.command()
@click.option("--pipeline-type", "-t", required=True)
@click.option(
    "--config",
    "-c",
    multiple=True,
    required=False,
    help="Configuration values for module initialization.",
)
@click.pass_context
def execution_graph(ctx, pipeline_type: str, config: typing.Iterable[typing.Any]):
    """Print the execution graph for a pipeline structure."""

    config = module_config_from_cli_args(*config)
    if config:
        print("Pipeline config not supported yet.")
        sys.exit()

    kiara_obj = ctx.obj["kiara"]

    if os.path.isfile(pipeline_type):
        pipeline_type = kiara_obj.register_pipeline_description(
            pipeline_type, raise_exception=True
        )

    m_cls = kiara_obj.get_module_class(pipeline_type)
    if not m_cls.is_pipeline():
        rich_print()
        rich_print(f"Module '{pipeline_type}' is not a pipeline-type module.")
        sys.exit(1)

    info = PipelineModuleInfo(module_type=pipeline_type)
    info.print_execution_graph()


@cli.command()
@click.pass_context
def dev(ctx):

    # kiara: Kiara = ctx.obj['kiara']
    #
    # # print('---')
    # workflow = kiara.create_workflow("network_analysis")
    #
    # workflow.inputs.nodes_info_path = "/home/markus/projects/dharpa/notebooks/NetworkXAnalysis/ReviewMasterTable.csv"
    # workflow.inputs.edges_path = "/home/markus/projects/dharpa/notebooks/NetworkXAnalysis/JournalEdges1902.csv"
    # workflow.inputs.source_column = "Source"
    # workflow.inputs.target_column = "Target"
    # workflow.inputs.weight_column = "weight"
    #
    # # for k, v in workflow.inputs.items():
    # #     print(f"{k}: {v.is_valid}")
    #
    # workflow.inputs.nodes_table_index = "Id"
    #
    # print(f"Workflow status: {workflow.status}")

    # print(workflow.get_current_state().json())
    # rich_print(workflow.outputs.data)
    # rich_print(workflow.outputs.graph)
    # print("---")
    # print(workflow.outputs.graph.get_value_data())

    # print(workflow.pipeline.get_current_state().json())

    # wf = kiara.create_workflow("nand")
    # wf.inputs.a = True
    # wf.inputs.b = False
    # j = wf.get_current_state().json()
    # print(j)

    kiara: Kiara = Kiara.instance()

    controller = BatchController()

    # print('---')
    workflow = kiara.create_workflow("assemble_network_graph", controller=controller)
    workflow = kiara.create_workflow("nand")

    workflow.get_current_state()

    # workflow.inputs.nodes_info_path = "/home/markus/projects/dharpa/notebooks/NetworkXAnalysis/ReviewMasterTable.csv"
    # workflow.inputs.edges_path = "/home/markus/projects/dharpa/notebooks/NetworkXAnalysis/JournalEdges1902.csv"
    # workflow.inputs.source_column = "Source"
    # workflow.inputs.target_column = "Target"
    # workflow.inputs.weight_column = "weight"
    # workflow.inputs.nodes_table_index = "Id"
    #
    # print_ascii_graph(workflow.structure.data_flow_graph)
    #
    # rich_print(workflow)
    #
    # rich_print(workflow.outputs.graph)
    # print(workflow.pipeline.get_current_state().json())


if __name__ == "__main__":
    cli()
