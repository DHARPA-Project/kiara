# -*- coding: utf-8 -*-
import asyncclick as click
from rich import box
from rich.table import Table

from kiara import Kiara
from kiara.operations import Operations


@click.group()
@click.pass_context
def operation(ctx):
    """Metadata-related sub-commands."""


# @operation.command(name="list")
# @click.pass_context
# def list_operations(ctx):
#
#     kiara_obj: Kiara = ctx.obj["kiara"]
#
#     op_mgmt = OperationMgmt(kiara=kiara_obj)
#
#     import pp
#
#     # pp(op_mgmt.profiles)
#
#     pp(op_mgmt.operation_types)


@operation.command(name="list")
@click.pass_context
def list_ops(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    all_operations_types = kiara_obj._operation_mgmt.operation_types

    table = Table(box=box.SIMPLE)
    table.add_column("Operation", no_wrap=True)
    table.add_column("Id", no_wrap=True)

    for operation_name in sorted(all_operations_types.keys()):

        operation_details: Operations = all_operations_types[operation_name]
        first_line_value = True

        for op_id, op_config in sorted(operation_details.operation_configs.items()):

            row = []
            if first_line_value:
                row.append(operation_name)
            else:
                row.append("")

            row.append(op_id)

            table.add_row(*row)
            first_line_value = False

    kiara_obj.explain(table)
