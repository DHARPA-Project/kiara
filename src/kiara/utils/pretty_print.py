# -*- coding: utf-8 -*-
import typing
from pyarrow import Table
from rich import box
from rich.console import RenderableType
from rich.table import Table as RichTable


def pretty_print_arrow_table(
    table: Table,
    num_head: typing.Optional[int] = None,
    num_tail: typing.Optional[int] = None,
) -> RenderableType:

    rich_table = RichTable(box=box.SIMPLE)
    for cn in table.column_names:
        rich_table.add_column(cn)

    num_split_rows = 1

    if num_head is not None:

        if num_head < 0:
            num_head = 0

        if num_head > table.num_rows:
            num_head = table.num_rows
            num_tail = None
            num_split_rows = 0

        if num_tail is not None:
            if num_head + num_tail >= table.num_rows:  # type: ignore
                num_head = table.num_rows
                num_tail = None
                num_split_rows = 0
    else:
        num_split_rows = 0

    if num_head is not None:
        head = table.slice(0, num_head)
        num_rows = num_head
    else:
        head = table
        num_rows = table.num_rows

    table_dict = head.to_pydict()
    for i in range(0, num_rows):
        row = []
        for cn in table.column_names:
            row.append(str(table_dict[cn][i]))

        rich_table.add_row(*row)

    if num_split_rows:
        for i in range(0, num_split_rows):
            row = []
            for _ in table.column_names:
                row.append("...")
            rich_table.add_row(*row)

    if num_head:
        if num_tail is not None:
            if num_tail < 0:
                num_tail = 0

            tail = table.slice(table.num_rows - num_tail)
            table_dict = tail.to_pydict()
            for i in range(0, num_rows):
                row = []
                for cn in table.column_names:
                    row.append(str(table_dict[cn][i]))

                rich_table.add_row(*row)

    return rich_table
