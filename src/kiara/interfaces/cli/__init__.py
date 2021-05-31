# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click
import os.path
import sys
import typing
from kiara_modules.core.json import DEFAULT_TO_JSON_CONFIG
from pathlib import Path
from rich import box
from rich.console import Console, RenderGroup
from rich.panel import Panel
from rich.syntax import Syntax

from kiara import Kiara
from kiara.interfaces import get_console
from kiara.module import KiaraModule, ModuleInfo
from kiara.pipeline.controller import BatchController
from kiara.pipeline.module import PipelineModuleInfo
from kiara.processing.parallel import ThreadPoolProcessor
from kiara.utils import create_table_from_field_schemas, dict_from_cli_args
from kiara.utils.output import OutputDetails

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
) -> KiaraModule:
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


@module.command(name="explain-type")
@click.argument("module_type", nargs=1, required=True)
@click.pass_context
def explain_module_type(ctx, module_type: str):
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


@module.command("explain-instance")
@click.argument("module_type", nargs=1)
@click.argument(
    "module_config",
    nargs=-1,
)
@click.pass_context
def explain_module(ctx, module_type: str, module_config: typing.Iterable[typing.Any]):
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


@cli.group(name="type")
@click.pass_context
def type_group(ctx):
    """Information about available modules, and details about them."""


@type_group.command(name="list")
@click.pass_context
def list_types(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    for type_name, type in kiara_obj.value_types.items():
        rich_print(f"{type_name}: {type}")


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
    "--workflow-details",
    "-d",
    help="Display additional workflow details.",
    is_flag=True,
)
@click.option(
    "--output", "-o", help="The output format and configuration.", multiple=True
)
@click.pass_context
async def run(ctx, module, inputs, module_config, output, workflow_details):

    if module_config:
        raise NotImplementedError()

    kiara_obj: Kiara = ctx.obj["kiara"]

    if module in kiara_obj.available_module_types:
        module_name = module
    elif f"core.{module}" in kiara_obj.available_module_types:
        module_name = f"core.{module}"
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

        module_obj: KiaraModule = _create_module_instance(
            ctx=ctx, module_type=module_name, module_config=module_config
        )

        one_required = False
        for input_name in module_obj.input_names:
            if module_obj.input_required(input_name):
                one_required = True
                break

        if one_required:

            inputs_table = create_table_from_field_schemas(
                _show_header=True, **module_obj.input_schemas
            )
            print()
            print(
                "No inputs provided, not running the workflow. To run it, provide input following this schema:"
            )
            rich_print(inputs_table)
            sys.exit(1)

    output_details = OutputDetails.from_data(output)
    if output_details.format != "terminal":
        pass

    force_overwrite = output_details.config.get("force", False)

    # SUPPORTED_TARGETS = ["terminal", "file"]
    # if output_details.target not in SUPPORTED_TARGETS:
    #     print()
    #     rich_print(f"Invalid output target '{output_details.target}', must be one of: [i]{', '.join(SUPPORTED_TARGETS)}[/i]")
    #     sys.exit(1)

    target_file: typing.Optional[Path] = None
    if output_details.target != "terminal":
        if output_details.target == "file":
            target_dir = Path.cwd()
            target_file = target_dir / f"{module_name}.{output_details.format}"
        else:
            target_file = Path(
                os.path.realpath(os.path.expanduser(output_details.target))
            )

        if target_file.exists() and not force_overwrite:
            print()
            print(
                f"Can't run workflow, the target files already exist, and '--output force=true' not specified: {target_file}"
            )
            sys.exit(1)

    processor = ThreadPoolProcessor()
    # processor = None
    controller = BatchController(processor=processor)
    workflow = kiara_obj.create_workflow(module_name, controller=controller)
    list_keys = []

    for name, value in workflow.inputs.items():
        if value.value_schema.type in ["array", "list"]:
            list_keys.append(name)

    workflow_input = dict_from_cli_args(*inputs, list_keys=list_keys)

    if workflow_input:
        workflow.inputs.set_values(**workflow_input)
    else:
        workflow.controller.process_pipeline()

    if workflow_details:
        kiara_obj.explain(workflow.current_state)

    if output_details.target == "terminal":
        if output_details.format == "terminal":
            print()
            pretty_print = kiara_obj.create_workflow("strings.pretty_print")
            pretty_print.inputs.set_value("item", workflow.outputs)

            renderables = pretty_print.outputs.get_value_data("renderables")
            output = Panel(RenderGroup(*renderables), box=box.SIMPLE)
            rich_print("[b]Output data[/b]")
            rich_print(output)
        else:

            format = output_details.format
            available_formats = kiara_obj.get_convert_target_types(
                source_type="value_set"
            )
            if format not in available_formats:
                print()
                print(
                    f"Can't convert to output format '{format}', this format is not supported. Available formats: {', '.join(available_formats)}."
                )
                sys.exit(1)

            config = {}
            config.update(DEFAULT_TO_JSON_CONFIG)

            transformed = kiara_obj.transform_data(
                workflow.outputs,
                source_type="value_set",
                target_type=format,
                config=config,
            )
            transformed_value = transformed.get_value_data("target_value")

            if format in ["json", "yaml"]:
                transformed_str = Syntax(
                    transformed_value, lexer_name=format, background_color="default"
                )
                rich_print(transformed_str)
            else:
                print(transformed_value)

    else:
        if output_details.format == "terminal":
            pretty_print = kiara_obj.create_workflow("strings.pretty_print")
            pretty_print.inputs.set_value("item", workflow.outputs)

            renderables = pretty_print.outputs.get_value_data("renderables")
            output = Panel(RenderGroup(*renderables), box=box.SIMPLE)
            with open(target_file, "wt") as f:
                console = Console(record=True, file=f)
                console.print(output)
        else:

            format = output_details.format
            available_formats = kiara_obj.get_convert_target_types(
                source_type="value_set"
            )
            if format not in available_formats:
                print()
                print(
                    f"Can't convert to output format '{format}', this format is not supported. Available formats: {', '.join(available_formats)}."
                )
                sys.exit(1)

            config = {}
            config.update(DEFAULT_TO_JSON_CONFIG)

            transformed = kiara_obj.transform_data(
                workflow.outputs,
                source_type="value_set",
                target_type=format,
                config=config,
            )
            transformed_value = transformed.get_value_data()

            target_file.parent.mkdir(parents=True, exist_ok=True)
            # TODO: check whether to write text or bytes
            target_file.write_text(transformed_value)


# @cli.command()
# @click.pass_context
# def dev(ctx):
#     import os
#
#     os.environ["DEBUG"] = "true"
#
#     kiara = Kiara.instance()
#
#     workflow = kiara.create_workflow("onboarding.import_local_folder")
#
#     extended = {
#         "steps": [
#             {
#                 "module_type": "tabular.create_table_from_text_files",
#                 "module_config": {
#                     "columns": ["id", "rel_path", "file_name", "content"]
#                 },
#                 "step_id": "create_table_from_files",
#             }
#         ],
#         "input_aliases": {"create_table_from_files__files": "file_bundle"},
#     }
#
#     structure = workflow.structure
#     new_structure = structure.extend(extended)
#
#     workflow = kiara.create_workflow("tabular.import_table_from_folder")
#     workflow.inputs.set_value("path", "/home/markus/projects/dharpa/data/csvs")
#
#     import pp
#
#     table = workflow.outputs.get_value_obj("table")
#     md = table.get_metadata()
#     pp(md)


if __name__ == "__main__":
    cli()
