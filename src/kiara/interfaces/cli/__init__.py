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
from kiara.workflow import KiaraWorkflow

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

    # test_pipelines_folder = os.path.abspath(
    #     os.path.join(
    #         os.path.dirname(__file__),
    #         "..",
    #         "..",
    #         "..",
    #         "..",
    #         "tests/resources/pipelines",
    #     )
    # )
    # test_pipeline_module_manager = PipelineModuleManager(test_pipelines_folder)
    # Kiara.instance().add_module_manager(test_pipeline_module_manager)


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

    if only_pipeline_modules:
        m_list = Kiara.instance().available_pipeline_module_types
    elif only_core_modules:
        m_list = Kiara.instance().available_non_pipeline_module_types
    else:
        m_list = Kiara.instance().available_module_types

    for name in m_list:
        rich_print(name)


@module.command(name="describe")
@click.argument("module_type", nargs=1, required=True)
@click.pass_context
def describe_module_type(ctx, module_type: str):
    """Print details of a (PYthon) module."""

    m_cls = Kiara.instance().get_module_class(module_type)
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

    m_cls = Kiara.instance().get_module_class(pipeline_module_type)
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

    m_cls = Kiara.instance().get_module_class(pipeline_module_type)
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

    config = module_config_from_cli_args(*config)

    module_obj = Kiara.instance().create_module(
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

    wf = KiaraWorkflow("xor")

    # pp(wf.pipeline.get_current_state().__dict__)
    print(wf.pipeline.get_current_state().json())

    # wf = KiaraWorkflow("logic_1")
    # wf = KiaraWorkflow("and")
    # import pp
    # pp(wf._workflow_config.__dict__)
    # print("XXXXXXXXXXX")
    # print(wf.structure.data_flow_graph.nodes)
    # print(graph_to_ascii(wf.structure.data_flow_graph))
    # pp(wf.__dict__)

    # cls = kiara.get_module_class("logic_1")
    # print(cls)

    # m = cls(id="test")
    # print(wf.input_names)
    # print(wf.output_names)

    # wc = KiaraWorkflowConfig.from_file(
    #     "/home/markus/projects/dharpa/kiara/tests/resources/workflows/logic_2.json"
    # )
    # # wc = KiaraWorkflowConfig(module_type="and")
    # wf = KiaraWorkflow(workflow_config=wc)
    #
    # # print_ascii_graph(wf.structure.data_flow_graph_simple)

    # wf.inputs.and_1__a = True
    # wf.inputs.and_1__b = True
    # wf.inputs.and_2__b = True
    # wf.inputs.and_1__a = True
    wf.inputs.a = True
    wf.inputs.b = False

    # print(wf.inputs)
    #
    # print(wf.state)
    #
    # print(wf.outputs.dict())

    # print(wf.outputs.and_2__y.get_value())
    # print(Kiara().instance().data_registry.get_stats())


if __name__ == "__main__":
    cli()
