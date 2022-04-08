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
from rich import box
from rich.panel import Panel
from rich.table import Table

from kiara import Kiara
from kiara.models.module.operation import OperationTypeClassesInfo
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

    operation_types_info = OperationTypeClassesInfo.create_from_items(
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
    "--by-type", "-t", is_flag=True, help="List the operations by operation type."
)
@click.argument("filter", nargs=-1, required=False)
@click.option(
    "--full-doc", "-d", is_flag=True, help="Display the full doc for all operations."
)
@click.option(
    "--omit-default",
    "-o",
    is_flag=True,
    help="Don't list operations that have no specific operation type associated with them.",
)
@click.option(
    "--include-internal-operations",
    "-i",
    help="Whether to include operations that are mainly used internally.",
    is_flag=True,
)
@click.pass_context
def list_operations(
    ctx,
    by_type: bool,
    filter: typing.Iterable[str],
    full_doc: bool,
    omit_default: bool,
    include_internal_operations: bool,
):

    kiara_obj: Kiara = ctx.obj["kiara"]

    if by_type:
        title = "Operations by type"
        all_operations_types = kiara_obj.operation_registry.operation_types

        table = Table(box=box.SIMPLE, show_header=True)
        table.add_column("Type", no_wrap=True, style="b green")
        table.add_column("Id", no_wrap=True)
        table.add_column("Description", no_wrap=False, style="i")

        for operation_name in sorted(all_operations_types.keys()):

            if operation_name == "custom_module":
                continue

            first_line_value = True

            for op_id in sorted(
                kiara_obj.operation_registry.operations_by_type[operation_name]
            ):
                operation = kiara_obj.operation_registry.get_operation(op_id)

                if (
                    not include_internal_operations
                    and operation.operation_details.is_internal_operation
                ):
                    continue
                if full_doc:
                    desc = operation.doc.full_doc
                else:
                    desc = operation.doc.description

                if filter:
                    match = True
                    for f in filter:
                        if (
                            f.lower() not in operation_name.lower()
                            and f.lower() not in op_id.lower()
                            and f.lower() not in desc.lower()
                        ):
                            match = False
                            break
                    if not match:
                        continue

                row = []
                if first_line_value:
                    row.append(operation_name)
                else:
                    row.append("")

                row.append(op_id)
                row.append(desc)

                table.add_row(*row)
                first_line_value = False

    else:
        title = "All operations"
        table = Table(box=box.SIMPLE, show_header=True)
        table.add_column("Id", no_wrap=True, style="b")
        table.add_column("Type(s)", style="green")
        table.add_column("Description", style="i")

        for op_id in kiara_obj.operation_registry.operation_ids:
            operation = kiara_obj.operation_registry.get_operation(op_id)

            if (
                not include_internal_operations
                and operation.operation_details.is_internal_operation
            ):
                continue

            types = kiara_obj.operation_registry.find_all_operation_types(
                operation.operation_id
            )
            if omit_default and len(types) == 1 and "all" in types:
                continue

            try:
                types.remove("custom_module")
            except KeyError:
                pass

            if full_doc:
                desc = operation.doc.full_doc
            else:
                desc = operation.doc.description

            if filter:
                match = True
                for f in filter:
                    if f.lower() not in op_id.lower() and f.lower() not in desc.lower():
                        match = False
                        break
                if match:
                    table.add_row(op_id, ", ".join(types), desc)

            else:
                table.add_row(op_id, ", ".join(types), desc)

    panel = Panel(table, title=title, title_align="left", box=box.ROUNDED)
    print()
    rich_print(panel)


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
