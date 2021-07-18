# -*- coding: utf-8 -*-
import asyncclick as click
from rich import box
from rich.table import Table

import kiara
from kiara import Kiara


@click.group()
@click.pass_context
def operation(ctx):
    """Metadata-related sub-commands."""


@operation.command(name="list")
@click.pass_context
def list_operations(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    all_operations = kiara_obj._operation_mgmt.operations

    # for value_type, operation_details in all_operations.items():
    #     print("===============")
    #     print(value_type)
    #     for operation_name, id_and_config in operation_details.items():
    #         print("-----------")
    #         print("  " + operation_name)
    #         for v1, v2 in id_and_config.items():
    #             pass
    #
    #             print("     " + v1)
    #             # pp(v2)

    table = Table(box=box.SIMPLE)
    table.add_column("Value type", no_wrap=True)
    table.add_column("Operation name", no_wrap=True)
    table.add_column("Operation id", no_wrap=True)

    for value_type in sorted(all_operations.keys()):

        operation_details = all_operations[value_type]
        first_line_value = True

        for operation_name in sorted(operation_details.keys()):
            id_and_config = operation_details[operation_name]
            first_line_op_name = True

            # cls = kiara_obj._operation_mgmt.get_operation_type_cls(operation_name)

            for o_id in sorted(id_and_config.keys()):

                row = []
                if first_line_value:
                    row.append(value_type)
                else:
                    row.append("")
                if first_line_op_name:
                    row.append(operation_name)
                else:
                    row.append("")
                row.append(o_id)

                table.add_row(*row)
                first_line_value = False
                first_line_op_name = False

    kiara.explain(table)
