# -*- coding: utf-8 -*-
import asyncclick as click
import os
import sys
import typing
from rich import box, print as rich_print
from rich.panel import Panel
from rich.table import Table

from kiara import Kiara
from kiara.operations import OperationType
from kiara.utils import log_message


@click.group()
@click.pass_context
def operation(ctx):
    """Metadata-related sub-commands."""


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
@click.pass_context
def list(
    ctx, by_type: bool, filter: typing.Iterable[str], full_doc: bool, omit_default: bool
):

    kiara_obj: Kiara = ctx.obj["kiara"]

    if by_type:
        title = "Operations by type"
        all_operations_types = kiara_obj.operation_mgmt.operation_types

        table = Table(box=box.SIMPLE, show_header=True)
        table.add_column("Type", no_wrap=True, style="b green")
        table.add_column("Id", no_wrap=True)
        table.add_column("Description", no_wrap=False, style="i")

        for operation_name in sorted(all_operations_types.keys()):

            if operation_name == "all":
                continue

            operation_details: OperationType = all_operations_types[operation_name]
            first_line_value = True

            for op_id, op_config in sorted(operation_details.operation_configs.items()):

                if full_doc:
                    desc = op_config.doc.full_doc
                else:
                    desc = op_config.doc.description

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

        for op_id, config in kiara_obj.operation_mgmt.profiles.items():

            types = kiara_obj.operation_mgmt.get_types_for_id(op_id)

            if omit_default and len(types) == 1:
                continue

            types.remove("all")

            if full_doc:
                desc = config.doc.full_doc
            else:
                desc = config.doc.description
            # desc = config.module_cls.get_type_metadata().documentation.description
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

    op_config = kiara_obj.operation_mgmt.profiles.get(operation_id)
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
