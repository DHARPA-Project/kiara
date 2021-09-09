# -*- coding: utf-8 -*-
"""Pipeline-related subcommands for the cli."""

import click
import os.path
import sys

from kiara.info.pipelines import PipelineModuleInfo
from kiara.utils.output import rich_print


@click.group()
@click.pass_context
def pipeline(ctx):
    """Pipeline-related sub-commands."""


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
