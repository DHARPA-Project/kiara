# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Pipeline-related subcommands for the cli."""

import os.path
import rich_click as click
import sys
import typing
from rich import box
from rich.table import Table

from kiara import Kiara
from kiara.models.module.operation import Operation, OperationGroupInfo
from kiara.models.module.pipeline import PipelineConfig
from kiara.modules.included_core_modules.pipeline import PipelineModule
from kiara.utils.cli import output_format_option, terminal_print_model
from kiara.utils.graphs import print_ascii_graph


def get_pipeline_config(kiara_obj: Kiara, pipeline_name_or_path: str) -> PipelineConfig:

    if os.path.isfile(pipeline_name_or_path):
        pc = PipelineConfig.from_file(pipeline_name_or_path, kiara=kiara_obj)
    else:
        operation: Operation = kiara_obj.operation_registry.get_operation(
            pipeline_name_or_path
        )
        pipeline_module: PipelineModule = operation.module  # type: ignore

        if not pipeline_module.is_pipeline():
            print()
            print(
                f"Specified operation id exists, but is not a pipeline: {pipeline_name_or_path}."
            )
            sys.exit(1)

        pc = pipeline_module.config

    return pc


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
@output_format_option()
@click.pass_context
def list_pipelines(ctx, full_doc: bool, filter: typing.Iterable[str], format: str):
    """List available module data_types."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    kiara_obj.operation_registry.get_operation_type("pipeline")

    table = Table(box=box.SIMPLE, show_header=True)
    table.add_column("Id", no_wrap=True)
    table.add_column("Description", no_wrap=False, style="i")

    op_ids = kiara_obj.operation_registry.operations_by_type["pipeline"]

    title = "Available pipelines"
    if filter:
        title = "Filtered pipelines"
        temp = {}
        for op_id in op_ids:
            op = kiara_obj.operation_registry.get_operation(op_id)
            match = True
            for f in filter:
                if f.lower() not in op_id.lower():
                    match = False
                    break
            if match:
                temp[op_id] = op
        operations = temp

    else:
        operations = {
            op_id: kiara_obj.operation_registry.get_operation(op_id) for op_id in op_ids
        }

    ops_info = OperationGroupInfo.create_from_operations(
        kiara=kiara_obj, group_alias=title, **operations
    )
    terminal_print_model(ops_info, format=format, in_panel=title, full_doc=full_doc)


@pipeline.command()
@click.argument("pipeline-id-or-path", nargs=1)
@output_format_option()
@click.pass_context
def explain(ctx, pipeline_name_or_path: str, format: str):
    """Print details about pipeline inputs, outputs, and overall structure."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    pc = get_pipeline_config(
        kiara_obj=kiara_obj, pipeline_name_or_path=pipeline_name_or_path
    )

    terminal_print_model(
        pc, format=format, in_panel=f"Pipeline: [b i]{pipeline_name_or_path}[/b i]"
    )


@pipeline.command()
@click.argument("pipeline-id-or-path", nargs=1)
@click.pass_context
def execution_graph(ctx, pipeline_name_or_path: str):
    """Print the execution graph for a pipeline structure."""

    kiara_obj = ctx.obj["kiara"]

    pc = get_pipeline_config(
        kiara_obj=kiara_obj, pipeline_name_or_path=pipeline_name_or_path
    )

    structure = pc.structure
    print_ascii_graph(structure.execution_graph)


@pipeline.command()
@click.argument("pipeline-id-or-path", nargs=1)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help="Display full data-flow graph, incl. intermediate input/output connections.",
)
@click.pass_context
def data_flow_graph(ctx, pipeline_name_or_path: str, full: bool):
    """Print the data flow graph for a pipeline structure."""

    kiara_obj = ctx.obj["kiara"]

    pc = get_pipeline_config(
        kiara_obj=kiara_obj, pipeline_name_or_path=pipeline_name_or_path
    )

    structure = pc.structure

    if full:
        print_ascii_graph(structure.data_flow_graph)
    else:
        print_ascii_graph(structure.data_flow_graph_simple)


# @pipeline.command()
# @click.argument("pipeline-type", nargs=1)
# @click.pass_context
# def explain_steps(ctx, pipeline_id: str):
#     """List all steps of a pipeline."""
#
#     kiara_obj = ctx.obj["kiara"]
#
#     if os.path.isfile(pipeline_id):
#         pipeline_id = kiara_obj.register_pipeline_description(
#             pipeline_id, raise_exception=True
#         )
#
#     m_cls = kiara_obj.get_module_class(pipeline_id)
#     if not m_cls.is_pipeline():
#         rich_print()
#         rich_print(f"Module '{pipeline_id}' is not a pipeline-type module.")
#         sys.exit(1)
#
#     info = PipelineModuleInfo.from_type_name(
#         module_type_name=pipeline_id, kiara=kiara_obj
#     )
#     print()
#     st_info = info.structure.steps_info
#     rich_print(st_info)
#
#
# @pipeline.command()
# @click.argument("pipeline-type", nargs=1)
# @click.pass_context
# def execution_graph(ctx, pipeline_id: str):
#     """Print the execution graph for a pipeline structure."""
#
#     kiara_obj = ctx.obj["kiara"]
#
#     if os.path.isfile(pipeline_id):
#         pipeline_id = kiara_obj.register_pipeline_description(
#             pipeline_id, raise_exception=True
#         )
#
#     m_cls = kiara_obj.get_module_class(pipeline_id)
#     if not m_cls.is_pipeline():
#         rich_print()
#         rich_print(f"Module '{pipeline_id}' is not a pipeline-type module.")
#         sys.exit(1)
#
#     info = PipelineModuleInfo.from_type_name(pipeline_id, kiara=kiara_obj)
#     info.print_execution_graph()
#
#
# @pipeline.command()
# @click.argument("pipeline-type", nargs=1)
# @click.option(
#     "--full",
#     "-f",
#     is_flag=True,
#     help="Display full data-flow graph, incl. intermediate input/output connections.",
# )
# @click.pass_context
# def data_flow_graph(ctx, pipeline_id: str, full: bool):
#     """Print the data flow graph for a pipeline structure."""
#
#     kiara_obj = ctx.obj["kiara"]
#     if os.path.isfile(pipeline_id):
#         pipeline_id = kiara_obj.register_pipeline_description(
#             pipeline_id, raise_exception=True
#         )
#
#     m_cls = kiara_obj.get_module_class(pipeline_id)
#     if not m_cls.is_pipeline():
#         rich_print()
#         rich_print(f"Module '{pipeline_id}' is not a pipeline-type module.")
#         sys.exit(1)
#
#     info = PipelineModuleInfo.from_type_name(pipeline_id, kiara=kiara_obj)
#
#     info.print_data_flow_graph(simplified=not full)
#
#
# try:
#     pass
#
#     @pipeline.command()
#     @click.argument("pipeline", nargs=1)
#     @click.option(
#         "--template",
#         "-t",
#         help="The template to use. Defaults to 'notebook'.",
#         default="notebook",
#     )
#     @click.pass_context
#     def render(ctx, pipeline, template):
#
#         kiara_obj: Kiara = ctx.obj["kiara"]
#
#         # pipeline = "/home/markus/projects/dharpa/kiara-playground/examples/streamlit/geolocation_prototype/pipelines/geolocation_1.yml"
#         # template = os.path.join(KIARA_RESOURCES_FOLDER, "templates", "python_script.py.j2")
#         rendered = kiara_obj.template_mgmt.render(
#             "pipeline", module=pipeline, template=template
#         )
#
#         print(rendered)
#
#
# except Exception:
#     log_message(
#         "'black' or 'jupytext' not installed, not adding 'pipeline render' subcommand."
#     )
