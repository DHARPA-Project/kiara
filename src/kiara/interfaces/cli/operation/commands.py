# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
import os
import rich_click as click
import sys
import typing
from rich.panel import Panel

from kiara import Kiara
from kiara.models.module.operation import OperationGroupInfo, OperationTypeClassesInfo
from kiara.utils import log_message, rich_print


@click.group()
@click.pass_context
def operation(ctx):
    """Metadata-related sub-commands."""


@operation.command("list-types")
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full documentation for every operation type (when using 'terminal' output format).",
)
@click.argument("filter", nargs=-1, required=False)
@click.option(
    "--format",
    "-f",
    help="The output format. Defaults to 'terminal'.",
    type=click.Choice(["terminal", "json", "html"]),
    default="terminal",
)
@click.pass_context
def list_types(ctx, full_doc, format: str, filter: typing.Iterable[str]):

    kiara_obj: Kiara = ctx.obj["kiara"]

    op_mgmt = kiara_obj.operation_registry

    op_types = op_mgmt.operation_type_classes

    title = "Available operation types"
    if filter:
        title = "Filtered data types"
        temp = {}
        for k, v in op_types.items():
            match = True
            for f in filter:
                if f.lower() not in k.lower():
                    match = False
                    break
            if match:
                temp[k] = v
        op_types = temp

    operation_types_info = OperationTypeClassesInfo.create_from_type_items(
        group_alias="all_items", **op_types
    )

    if format == "terminal":
        print()
        p = Panel(
            operation_types_info.create_renderable(full_doc=full_doc),
            title_align="left",
            title=title,
        )
        rich_print(p)
    elif format == "json":
        print(operation_types_info.json(option=orjson.OPT_INDENT_2))
    elif format == "html":
        print(operation_types_info.create_html())


@operation.command(name="list")
@click.option(
    "--by-type",
    "-t",
    is_flag=True,
    help="List the operations by operation type (when using 'terminal' as format).",
)
@click.argument("filter", nargs=-1, required=False)
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full doc for all operations (when using 'terminal' as format).",
)
@click.option(
    "--include-internal-operations",
    "-i",
    help="Whether to include operations that are mainly used internally.",
    is_flag=True,
)
@click.option(
    "--format",
    "-f",
    help="The output format. Defaults to 'terminal'.",
    type=click.Choice(["terminal", "json", "html"]),
    default="terminal",
)
@click.pass_context
def list_operations(
    ctx,
    by_type: bool,
    filter: typing.Iterable[str],
    full_doc: bool,
    include_internal_operations: bool,
    format: str,
):

    kiara_obj: Kiara = ctx.obj["kiara"]

    operations = kiara_obj.operation_registry.operations
    title = "Available operations"
    if filter:
        title = "Filtered operations"
        temp = {}
        for op_id, op in operations.items():
            match = True
            for f in filter:
                if f.lower() not in op_id.lower():
                    match = False
                    break
            if match:
                temp[op_id] = op
        operations = temp

    if not include_internal_operations:
        temp = {}
        for op_id, op in operations.items():
            if not op.operation_details.is_internal_operation:
                temp[op_id] = op

        operations = temp

    ops_info = OperationGroupInfo.create_from_operations(
        kiara=kiara_obj, group_alias=title, **operations
    )

    if format == "terminal":
        print()
        rich_print(
            ops_info.create_renderable(
                full_doc=full_doc,
                by_type=by_type,
                include_internal_operations=include_internal_operations,
            )
        )
    elif format == "json":
        print(ops_info.json(option=orjson.OPT_INDENT_2))
    elif format == "html":
        print(ops_info.create_html())


@operation.command()
@click.argument("operation_id", nargs=1, required=True)
@click.option(
    "--source",
    "-s",
    help="Show module source code (or pipeline configuration).",
    is_flag=True,
)
@click.pass_context
def explain(ctx, operation_id: str, source: bool):

    kiara_obj: Kiara = ctx.obj["kiara"]

    if os.path.isfile(os.path.realpath(operation_id)):
        try:
            _operation_id = kiara_obj.register_pipeline_description(data=operation_id)
            if _operation_id is None:
                print(
                    f"Unknown error when trying to import '{operation_id}' as pipeline."
                )
                sys.exit(1)
            else:
                operation_id = _operation_id
        except Exception as e:
            log_message(f"Tried to import '{operation_id}' as pipeline, failed: {e}")

    op_config = kiara_obj.operation_registry.get_operation(operation_id)
    if not op_config:
        print()
        print(f"No operation with id '{operation_id}' registered.")
        sys.exit(1)

    print()
    rich_print(
        op_config.create_panel(
            title=f"Operation: [b i]{operation_id}[/b i]", include_src=source
        )
    )
