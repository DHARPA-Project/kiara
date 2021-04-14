# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click
import sys
import typing
from rich import print as rich_print

from kiara import Kiara
from kiara.module import ModuleInfo
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
    pass


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


@module.command(name="describe")
@click.argument("module_type", nargs=1, required=True)
@click.pass_context
def describe_module_type(ctx, module_type: str):
    """Print details of a (PYthon) module."""

    kiara_obj = ctx.obj["kiara"]

    m_cls = kiara_obj.get_module_class(module_type)
    if module_type == "pipeline" or not m_cls.is_pipeline():
        info = ModuleInfo(module_type=module_type)
    else:
        info = PipelineModuleInfo(module_type=module_type)
    rich_print()
    rich_print(info)


@cli.group()
@click.pass_context
def pipeline(ctx):
    """Pipeline-related sub-commands."""


@pipeline.command()
@click.argument("pipeline_module_type", nargs=1)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help="Display full data-flow graph, incl. intermediate input/output connections.",
)
@click.pass_context
def data_flow_graph(ctx, pipeline_module_type: str, full: bool):
    """Print the data flow graph for a pipeline structure."""

    kiara_obj = ctx.obj["kiara"]

    m_cls = kiara_obj.get_module_class(pipeline_module_type)
    if not m_cls.is_pipeline():
        rich_print()
        rich_print(f"Module '{pipeline_module_type}' is not a pipeline-type module.")
        sys.exit(1)

    info = PipelineModuleInfo(module_type=pipeline_module_type)

    info.print_data_flow_graph(simplified=not full)


@pipeline.command()
@click.argument("pipeline_module_type", nargs=1)
@click.pass_context
def execution_graph(ctx, pipeline_module_type: str):
    """Print the execution graph for a pipeline structure."""

    kiara_obj = ctx.obj["kiara"]

    m_cls = kiara_obj.get_module_class(pipeline_module_type)
    if not m_cls.is_pipeline():
        rich_print()
        rich_print(f"Module '{pipeline_module_type}' is not a pipeline-type module.")
        sys.exit(1)

    info = PipelineModuleInfo(module_type=pipeline_module_type)
    info.print_execution_graph()


@cli.group()
@click.pass_context
def step(ctx):
    """Display instantiated module details."""


@step.command("describe")
@click.option("--module-type", "-t", nargs=1)
@click.option(
    "--config",
    "-c",
    multiple=True,
    required=False,
    help="Configuration values for module initialization.",
)
@click.pass_context
def describe_step(ctx, module_type: str, config: typing.Iterable[typing.Any]):
    """Describe a step.

    A step, in this context, is basically a an instantiated module class, incl. (optional) config."""

    config = module_config_from_cli_args(*config)

    kiara_obj = ctx.obj["kiara"]
    module_obj = kiara_obj.create_module(
        id=module_type, module_type=module_type, module_config=config
    )
    rich_print()
    rich_print(module_obj)


@cli.command()
@click.pass_context
def dev(ctx):

    # main_module = "kiara"

    # md_obj: ProjectMetadata = ProjectMetadata(project_main_module=main_module)
    #
    # md_json = json.dumps(
    #     md_obj.to_dict(), sort_keys=True, indent=2, separators=(",", ": ")
    # )
    # print(md_json)

    # for entry_point_group, eps in entry_points().items():
    #     print(entry_point_group)
    #     print(eps)

    # pc = get_data_from_file(
    #     "/home/markus/projects/dharpa/kiara/tests/resources/workflows/logic_1.json"
    # )
    # wc = KiaraWorkflowConfig(module_config=pc)

    # kiara = Kiara.instance()
    # print(kiara)

    # wf = KiaraWorkflow(
    #     "/home/markus/projects/dharpa/kiara/tests/resources/workflows/logic/logic_2.json"
    # )

    # wf = KiaraWorkflow(
    #     "/home/markus/projects/dharpa/kiara/tests/resources/workflows/dummy/dummy_1_delay.json"
    # )

    kiara_obj = ctx.obj["kiara"]

    wf = kiara_obj.create_workflow("xor")

    print(wf.pipeline.get_current_state().json())

    wf.inputs.a = True
    wf.inputs.b = False

    print(wf.status)

    rich_print(wf.get_current_state().dict())


if __name__ == "__main__":
    cli()
