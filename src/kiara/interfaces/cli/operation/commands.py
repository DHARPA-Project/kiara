# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import rich_click as click
import sys
import typing

from kiara import Kiara
from kiara.interfaces.python_api.operation import KiaraOperation
from kiara.models.module.operation import OperationGroupInfo, OperationTypeClassesInfo
from kiara.utils.cli import output_format_option, terminal_print_model


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
@output_format_option()
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

    terminal_print_model(operation_types_info, format=format, in_panel=title)


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
    "--include-internal",
    "-I",
    help="Whether to include operations that are mainly used internally.",
    is_flag=True,
)
@output_format_option()
@click.pass_context
def list_operations(
    ctx,
    by_type: bool,
    filter: typing.Iterable[str],
    full_doc: bool,
    include_internal: bool,
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

    if not include_internal:
        temp = {}
        for op_id, op in operations.items():
            if not op.operation_details.is_internal_operation:
                temp[op_id] = op

        operations = temp

    ops_info = OperationGroupInfo.create_from_operations(
        kiara=kiara_obj, group_alias=title, **operations
    )

    terminal_print_model(
        ops_info,
        format=format,
        in_panel=title,
        include_internal_operations=include_internal,
        full_doc=full_doc,
        by_type=by_type,
    )


@operation.command()
@click.argument("operation_id", nargs=1, required=True)
@click.option(
    "--source",
    "-s",
    help="Show module source code (or pipeline configuration).",
    is_flag=True,
)
@click.option(
    "--module-info", "-m", help="Show module type and config information.", is_flag=True
)
@output_format_option()
@click.pass_context
def explain(ctx, operation_id: str, source: bool, format: str, module_info: bool):

    kiara_obj: Kiara = ctx.obj["kiara"]

    if os.path.isfile(os.path.realpath(operation_id)):
        kiara_op = KiaraOperation(kiara=kiara_obj, operation_name=operation_id)
        op_config = kiara_op.operation
    else:
        op_config = kiara_obj.operation_registry.get_operation(operation_id)

    if not op_config:
        print()
        print(f"No operation with id '{operation_id}' registered.")
        sys.exit(1)

    terminal_print_model(
        op_config,
        format=format,
        in_panel=f"Operation: [b i]{operation_id}[/b i]",
        include_src=source,
        include_module_details=module_info,
    )
