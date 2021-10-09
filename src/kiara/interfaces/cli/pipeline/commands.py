# -*- coding: utf-8 -*-
"""Pipeline-related subcommands for the cli."""
import click
import os.path
import sys
import typing
from rich.panel import Panel

from kiara import Kiara
from kiara.defaults import KIARA_RESOURCES_FOLDER
from kiara.info.modules import ModuleTypesGroupInfo
from kiara.info.pipelines import PipelineModuleInfo
from kiara.utils import log_message
from kiara.utils.output import rich_print


@click.group()
@click.pass_context
def pipeline(ctx):
    """Pipeline-related sub-commands."""


@pipeline.command(name="list")
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full documentation for every module type.",
)
@click.argument("filter", nargs=-1, required=False)
@click.pass_context
def list_pipelines(
    ctx,
    full_doc: bool,
    filter: typing.Iterable[str],
):
    """List available module types."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    if filter:
        module_types = []

        for m in kiara_obj.available_pipeline_module_types:
            match = True

            for f in filter:

                if f.lower() not in m.lower():
                    match = False
                    break
                else:
                    m_cls = kiara_obj.get_module_class(m)
                    doc = m_cls.get_type_metadata().documentation.full_doc

                    if f.lower() not in doc.lower():
                        match = False
                        break

            if match:
                module_types.append(m)
    else:
        module_types = kiara_obj.available_pipeline_module_types

    renderable = ModuleTypesGroupInfo.create_renderable_from_type_names(
        kiara=kiara_obj,
        type_names=module_types,
        ignore_non_pipeline_modules=True,
        ignore_pipeline_modules=False,
        include_full_doc=full_doc,
    )
    title = "Available pipeline modules"

    p = Panel(renderable, title_align="left", title=title)
    print()
    kiara_obj.explain(p)


@pipeline.command()
@click.argument("pipeline-type", nargs=1)
@click.pass_context
def explain(ctx, pipeline_type: str):
    """Print details about pipeline inputs, outputs, and overall structure."""

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

    info = PipelineModuleInfo.from_type_name(pipeline_type, kiara=kiara_obj)
    print()
    kiara_obj.explain(info.structure)


@pipeline.command()
@click.argument("pipeline-type", nargs=1)
@click.pass_context
def explain_steps(ctx, pipeline_type: str):
    """List all steps of a pipeline."""

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

    info = PipelineModuleInfo.from_type_name(
        module_type_name=pipeline_type, kiara=kiara_obj
    )
    print()
    st_info = info.structure.steps_info
    rich_print(st_info)


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

    info = PipelineModuleInfo.from_type_name(pipeline_type, kiara=kiara_obj)
    info.print_execution_graph()


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

    info = PipelineModuleInfo.from_type_name(pipeline_type, kiara=kiara_obj)

    info.print_data_flow_graph(simplified=not full)


try:
    import black
    import jupytext

    @pipeline.command()
    @click.argument("pipeline", nargs=1)
    @click.option(
        "--template",
        "-t",
        help="The template to use. Defaults to 'notebook'.",
        default="notebook",
    )
    @click.pass_context
    def render(ctx, pipeline, template):

        kiara_obj: Kiara = ctx.obj["kiara"]

        # pipeline = "/home/markus/projects/dharpa/kiara-playground/examples/streamlit/geolocation_prototype/pipelines/geolocation_1.yml"
        # template = os.path.join(KIARA_RESOURCES_FOLDER, "templates", "python_script.py.j2")
        rendered = kiara_obj.template_mgmt.render(
            "pipeline", module=pipeline, template=template
        )

        print(rendered)


except Exception:
    log_message(
        "'black' or 'jupytext' not installed, not adding 'pipeline render' subcommand."
    )
