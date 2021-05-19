# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click
import os.path
import sys
import typing
from rich.panel import Panel

from kiara import Kiara
from kiara.data.values import ValuesInfo
from kiara.interfaces import get_console
from kiara.module import KiaraModule, ModuleInfo
from kiara.pipeline.controller import BatchController
from kiara.pipeline.module import PipelineModuleInfo
from kiara.processing.parallel import ThreadPoolProcessor
from kiara.utils import create_table_from_field_schemas, dict_from_cli_args

# from importlib.metadata import entry_points


# from asciinet import graph_to_ascii


try:
    import uvloop

    uvloop.install()
except Exception:
    pass

click.anyio_backend = "asyncio"


def rich_print(msg: typing.Any = None) -> None:

    if msg is None:
        msg = ""
    console = get_console()
    console.print(msg)


def _create_module_instance(
    ctx, module_type: str, module_config: typing.Iterable[typing.Any]
):
    config = dict_from_cli_args(*module_config)

    kiara_obj = ctx.obj["kiara"]
    if os.path.isfile(module_type):
        module_type = kiara_obj.register_pipeline_description(
            module_type, raise_exception=True
        )

    module_obj = kiara_obj.create_module(
        id=module_type, module_type=module_type, module_config=config
    )
    return module_obj


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

    kiara_obj: Kiara = ctx.obj["kiara"]

    if only_pipeline_modules:
        title = "Available pipeline modules"
        m_list = kiara_obj.create_modules_list(
            list_pipeline_modules=True, list_non_pipeline_modules=False
        )
    elif only_core_modules:
        title = "Available core modules"
        m_list = kiara_obj.create_modules_list(
            list_pipeline_modules=False, list_non_pipeline_modules=True
        )
    else:
        title = "Available modules"
        m_list = kiara_obj.modules_list

    p = Panel(m_list, title_align="left", title=title)
    print()
    kiara_obj.explain(p)


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


@module.command("describe-instance")
@click.argument("module_type", nargs=1)
@click.argument(
    "module_config",
    nargs=-1,
)
@click.pass_context
def describe_module(ctx, module_type: str, module_config: typing.Iterable[typing.Any]):
    """Describe a step.

    A step, in this context, is basically a an instantiated module class, incl. (optional) config."""

    module_obj = _create_module_instance(
        ctx, module_type=module_type, module_config=module_config
    )
    rich_print()
    rich_print(module_obj)


@cli.group()
@click.pass_context
def pipeline(ctx):
    """Pipeline-related sub-commands."""


@pipeline.command()
@click.argument("pipeline-type", nargs=1)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help="Display full data-flow graph, incl. intermediate input/output connections.",
)
@click.pass_context
def data_flow_graph(ctx, pipeline_type: str, full: bool):
    """Print the data flow graph for a pipeline structure."""

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
@click.argument("pipeline-type", nargs=1)
@click.pass_context
def execution_graph(ctx, pipeline_type: str):
    """Print the execution graph for a pipeline structure."""

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


@pipeline.command()
@click.argument("pipeline-type", nargs=1)
@click.pass_context
def structure(ctx, pipeline_type: str):
    """Print details about a pipeline structure."""

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

    info = PipelineModuleInfo(module_type=pipeline_type, _kiara=kiara_obj)
    structure = info.create_structure()
    print()
    kiara_obj.explain(structure)


@pipeline.command()
@click.argument("pipeline-type", nargs=1)
@click.pass_context
def explain_steps(ctx, pipeline_type: str):

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
    structure = info.create_structure()
    print()
    kiara_obj.explain(structure.to_details().steps_info)


@cli.command()
@click.argument("module", nargs=1)
@click.argument("inputs", nargs=-1, required=False)
@click.option(
    "--module-config",
    "-c",
    required=False,
    help="(Optional) module configuration.",
    multiple=True,
)
@click.option(
    "--data-details",
    "-d",
    help="Print details/metadata about input/output data.",
    is_flag=True,
    default=False,
)
@click.option(
    "--only-output",
    "-o",
    help="Only print output data. Overrides all other display options.",
    is_flag=True,
    default=False,
)
@click.pass_context
async def run(ctx, module, inputs, module_config, data_details, only_output):

    if module_config:
        raise NotImplementedError()

    display_state = True
    display_input_values = True
    display_input_details = False
    display_output_values = True
    display_output_details = False

    if data_details:
        display_input_details = True
        display_output_details = True

    if only_output:
        display_state = False
        display_input_values = False
        display_input_details = False

    kiara_obj: Kiara = ctx.obj["kiara"]

    if module in kiara_obj.available_module_types:
        module_name = module
    elif os.path.isfile(module):
        module_name = kiara_obj.register_pipeline_description(
            module, raise_exception=True
        )
    else:
        rich_print(
            f"\nInvalid module name '[i]{module}[/i]'. Must be a path to a pipeline file, or one of the available modules:\n"
        )
        for n in kiara_obj.available_module_types:
            rich_print(f"  - [i]{n}[/i]")
        sys.exit(1)

    if not inputs:
        print()
        print(
            "No inputs provided, not running the workflow. To run it, provide input following this schema:"
        )
        module_obj: KiaraModule = _create_module_instance(
            ctx=ctx, module_type=module_name, module_config=module_config
        )
        inputs_table = create_table_from_field_schemas(
            _show_header=True, **module_obj.input_schemas
        )
        rich_print(inputs_table)
        sys.exit(0)

    processor = ThreadPoolProcessor()
    processor = None
    controller = BatchController(processor=processor)

    workflow = kiara_obj.create_workflow(module_name, controller=controller)

    # l = DebugListener()
    # workflow.pipeline.add_listener(l)

    list_keys = []
    for name, value in workflow.inputs.items():
        if value.value_schema.type in ["array", "list"]:
            list_keys.append(name)

    workflow_input = dict_from_cli_args(*inputs, list_keys=list_keys)
    workflow.inputs.set_values(**workflow_input)

    transformer = "to_string"
    transformer_config = {"max_lines": 6}
    print()
    if display_input_values:
        vi = ValuesInfo(workflow.inputs)
        vt = vi.create_value_data_table(
            show_headers=True,
            transformer=transformer,
            transformer_config=transformer_config,
        )
        rich_print(Panel(vt, title_align="left", title="Workflow input data"))
        print()

    if display_input_details:
        vi = ValuesInfo(workflow.inputs)
        vt = vi.create_value_info_table(show_headers=True)
        rich_print(Panel(vt, title_align="left", title="Workflow input details"))
        print()

    if display_state:
        state_panel = Panel(
            workflow.current_state,
            title="Workflow state",
            title_align="left",
            padding=(1, 0, 0, 2),
        )
        rich_print(state_panel)
        print()

    if display_output_details:
        vi = ValuesInfo(workflow.outputs)
        vt = vi.create_value_info_table(show_headers=True)
        rich_print(Panel(vt, title_align="left", title="Workflow output details"))
        print()

    if display_output_values:
        vi = ValuesInfo(workflow.outputs)
        vt = vi.create_value_data_table(
            show_headers=True,
            transformer=transformer,
            transformer_config=transformer_config,
        )
        rich_print(Panel(vt, title_align="left", title="Workflow output data"))


# @cli.command()
# @click.pass_context
# def dev(ctx):
#     import os
#
#     os.environ["DEBUG"] = "true"
#
#     # kiara = Kiara.instance()
#
#     ctx = Context.instance()
#
#     sub = ctx.socket(zmq.SUB)
#     sub.setsockopt_string(zmq.SUBSCRIBE, "")
#     sub.connect("tcp://127.0.0.1:5556")
#     while True:
#         message = sub.recv()
#         print("Received request: %s" % message)
#         # socket.send(b"World")
#
#     module_info = kiara.get_module_info("import_local_folder")
#     # kiara.explain(module_info)
#
#     workflow = kiara.create_workflow("import_local_folder")
#     workflow.inputs.path = (
#         "/home/markus/projects/dharpa/notebooks/TopicModelling/data_tm_workflow"
#     )
#
#     # kiara.explain(workflow.outputs)
#     kiara.explain(workflow.outputs.file_bundle)
#

# @cli.command()
# @click.pass_context
# def dev2(ctx):
#     import os
#
#     os.environ["DEBUG"] = "true"
#
#     # kiara = Kiara.instance()
#
#     ctx = Context.instance()
#
#     pub = ctx.socket(zmq.PUB)
#     pub.connect("tcp://127.0.0.1:5555")
#     while True:
#         topic = random.randrange(9999, 10005)
#         messagedata = random.randrange(1, 215) - 80
#         print("%d %d" % (topic, messagedata))
#         pub.send_string("%d %d" % (topic, messagedata))
#         time.sleep(1)
#
#     module_info = kiara.get_module_info("import_local_folder")
#     # kiara.explain(module_info)
#
#     workflow = kiara.create_workflow("import_local_folder")
#     workflow.inputs.path = (
#         "/home/markus/projects/dharpa/notebooks/TopicModelling/data_tm_workflow"
#     )
#
#     # kiara.explain(workflow.outputs)
#     kiara.explain(workflow.outputs.file_bundle)


if __name__ == "__main__":
    cli()
