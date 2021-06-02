# -*- coding: utf-8 -*-

"""A command-line interface for *Kiara*.
"""
import asyncclick as click
import json
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
from kiara.data.values import Value, ValuesInfo
from kiara.interfaces import get_console
from kiara.module import KiaraModule, ModuleInfo
from kiara.pipeline.controller import BatchController
from kiara.pipeline.module import PipelineModuleInfo
from kiara.pipeline.pipeline import StepStatus
from kiara.processing.parallel import ThreadPoolProcessor
from kiara.utils import create_table_from_field_schemas, dict_from_cli_args, is_debug
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
    """List available module types."""

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
    """Print details of a module type.

    This is different to the 'explain-instance' command, because module types need to be
    instantiated with configuration, before we can query all their properties (like
    input/output types).
    """

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
    """Describe a module instance.

    This command shows information and metadata about an instantiated *kiara* module.
    """

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
@click.option("--id", "-i", help="Set workflow id.", required=False)
@click.option(
    "--module-config",
    "-c",
    required=False,
    help="(Optional) module configuration.",
    multiple=True,
)
@click.option(
    "--explain",
    "-e",
    help="Display additional workflow details.",
    is_flag=True,
)
@click.option(
    "--output", "-o", help="The output format and configuration.", multiple=True
)
@click.option(
    "--save", "-s", help="Save the outputs into the kiara data store.", is_flag=True
)
@click.pass_context
async def run(ctx, module, inputs, module_config, output, explain, save, id):
    """Execute a workflow run."""

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
            sys.exit(0)

    output_details = OutputDetails.from_data(output)
    silent = False
    if output_details.format == "silent":
        silent = True

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
    processor = None
    controller = BatchController(processor=processor)

    workflow_id = id
    if workflow_id is None:
        workflow_id = f"{module_name}_0"

    workflow = kiara_obj.create_workflow(
        module_name, workflow_id=workflow_id, controller=controller
    )

    if save:

        invalid = set()
        for ov in workflow.outputs.values():
            existing = kiara_obj.persitence.check_existing_aliases(*ov.aliases)
            invalid.update(existing)

        if invalid:
            print()
            print(
                f"Can't run workflow, value aliases for saving already exist: {', '.join(invalid)}. Set another workflow id?"
            )
            sys.exit(1)

    list_keys = []

    for name, value in workflow.inputs.items():
        if value.value_schema.type in ["array", "list"]:
            list_keys.append(name)

    workflow_input = dict_from_cli_args(*inputs, list_keys=list_keys)
    failed = False
    try:
        if workflow_input:
            workflow.inputs.set_values(**workflow_input)
        else:
            workflow.controller.process_pipeline()
    except Exception as e:
        print()
        print(e)
        failed = True

    if explain:
        print()
        kiara_obj.explain(workflow.current_state)

        if workflow.status == StepStatus.RESULTS_READY:
            vi = ValuesInfo(workflow.outputs)
            vi_table = vi.create_value_info_table(
                ensure_metadata=True, show_headers=True
            )
            panel = Panel(Panel(vi_table), box=box.SIMPLE)
            rich_print("[b]Output data details[/b]")
            rich_print(panel)

    if failed:
        sys.exit(1)

    if not silent:

        if output_details.target == "terminal":
            if output_details.format == "terminal":
                print()
                pretty_print = kiara_obj.create_workflow("strings.pretty_print")
                pretty_print.inputs.set_value("item", workflow.outputs)

                renderables = pretty_print.outputs.get_value_data("renderables")
                if renderables:
                    output = Panel(RenderGroup(*renderables), box=box.SIMPLE)
                    rich_print("[b]Output data[/b]")
                    rich_print(output)
                else:
                    rich_print("No output.")
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

                try:
                    transformed = kiara_obj.transform_data(
                        workflow.outputs,
                        source_type="value_set",
                        target_type=format,
                        config=config,
                    )
                    transformed_value = transformed.get_value_data("target_value")

                    if format in ["json", "yaml"]:
                        transformed_str = Syntax(
                            transformed_value,
                            lexer_name=format,
                            background_color="default",
                        )
                        rich_print(transformed_str)
                    else:
                        print(transformed_value)
                except Exception as e:
                    print()
                    rich_print(f"Can't transform outputs into '{format}': {e}")
                    sys.exit(1)

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

    if save:
        for field, value in workflow.outputs.items():
            rich_print(f"Saving '[i]{field}[/i]'...")
            try:
                value_id = value.save()
                rich_print(f"   -> done, id: [i]{value_id}[/i]")

            except Exception as e:
                if is_debug():
                    import traceback

                    traceback.print_exc()
                rich_print(f"   -> failed: [red]{e}[/red]")
            print()


@cli.group()
@click.pass_context
def data(ctx):
    """Data-related sub-commands."""


@data.command(name="list")
@click.option("--details", "-d", help="Display data item details.", is_flag=True)
@click.option("--ids", "-i", help="List value ids instead of aliases.", is_flag=True)
@click.pass_context
def list_values(ctx, details, ids):

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    if ids:
        for id, d in kiara_obj.persitence.values_metadata.items():
            if not details:
                rich_print(f"  - [b]{id}[/b]: {d['type']}")
            else:
                rich_print(f"[b]{id}[/b]: {d['type']}\n")
                md = kiara_obj.persitence.get_value_metadata(value_id=id)
                s = Syntax(json.dumps(md, indent=2), "json")
                rich_print(s)
                print()
    else:
        for alias, v_id in kiara_obj.persitence.aliases.items():
            v_type = kiara_obj.persitence.get_value_type(v_id)
            if not details:
                rich_print(f"  - [b]{alias}[/b]: {v_type}")
            else:
                rich_print(f"[b]{alias}[/b]: {v_type}\n")
                md = kiara_obj.persitence.get_value_metadata(value_id=v_id)
                s = Syntax(json.dumps(md, indent=2), "json")
                rich_print(s)
                print()


@data.command(name="explain")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def explain_value(ctx, value_id: str):

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value = kiara_obj.persitence.load_value(value_id=value_id)
    rich_print(value)


@data.command(name="load")
@click.argument("value_id", nargs=1, required=True)
@click.pass_context
def load_value(ctx, value_id: str):

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    value = kiara_obj.persitence.load_value(value_id=value_id)

    renderables: Value = kiara_obj.run(  # type: ignore
        "strings.pretty_print", inputs={"item": value}, output_name="renderables"
    )
    rich_print(*renderables.get_value_data())


@cli.group(name="type")
@click.pass_context
def type_group(ctx):
    """Information about available value types, and details about them."""


@type_group.command(name="list")
@click.pass_context
def list_types(ctx):
    """List available types (work in progress)."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    print()
    for type_name, type in kiara_obj.value_types.items():
        rich_print(f"{type_name}: {type}")


# @cli.command()
# @click.pass_context
# def dev(ctx):
#     import os
#
#     os.environ["DEBUG"] = "true"
#
#     kiara = Kiara.instance()
#
#     workflow = kiara.create_workflow("network.graphs.import_network_graph")
#
#     workflow.inputs.set_values(
#         edges_path="/home/markus/projects/dharpa/notebooks/NetworkXAnalysis/JournalEdges1902.csv",
#         source_column="Source",
#         target_column="Target",
#     )
#
#     graph = workflow.outputs.get_value_obj("graph")
#
#     result = graph.save()
#     load_config = result["metadata"]["load_config"]["metadata"]
#     lc = LoadConfig(**load_config)
#
#     r2 = kiara.run(**lc.dict())
#
#     kiara.explain(r2)
#
#     # inputs = result.pop("inputs")
#     # wf = kiara.create_workflow(config=result)
#     #
#     # wf.inputs.set_values(**inputs)
#     # kiara.explain(wf.current_state)
#     #
#     # print(wf.outputs.get_all_value_data())


if __name__ == "__main__":
    cli()
