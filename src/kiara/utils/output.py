# -*- coding: utf-8 -*-
import typing
from pyarrow import Table
from pydantic import BaseModel, Field, root_validator
from rich import box
from rich.console import RenderableType
from rich.table import Table as RichTable

from kiara.utils import dict_from_cli_args


class OutputDetails(BaseModel):
    @classmethod
    def from_data(cls, data: typing.Any):

        if isinstance(data, str):
            if "=" in data:
                data = [data]
            else:
                data = [f"format={data}"]

        if isinstance(data, typing.Iterable):
            data = list(data)
            if len(data) == 1 and isinstance(data[0], str) and "=" not in data[0]:
                data = [f"format={data[0]}"]
            output_details_dict = dict_from_cli_args(*data)
        else:
            raise TypeError(
                f"Can't parse output detail config: invalid input type '{type(data)}'."
            )

        output_details = OutputDetails(**output_details_dict)
        return output_details

    format: str = Field(description="The output format.")
    target: str = Field(description="The output target.")
    config: typing.Dict[str, typing.Any] = Field(
        description="Output configuration.", default_factory=dict
    )

    @root_validator(pre=True)
    def _set_defaults(cls, values):

        target: str = values.pop("target", "terminal")
        format: str = values.pop("format", None)
        if format is None:
            if target == "terminal":
                format = "terminal"
            else:
                if target == "file":
                    format = "json"
                else:
                    ext = target.split(".")[-1]
                    if ext in ["yaml", "json"]:
                        format = ext
                    else:
                        format = "json"
        result = {"format": format, "target": target, "config": dict(values)}

        return result


def pretty_print_arrow_table(
    table: Table,
    rows_head: typing.Optional[int] = None,
    rows_tail: typing.Optional[int] = None,
    max_row_height: typing.Optional[int] = None,
    max_cell_length: typing.Optional[int] = None,
) -> RenderableType:

    rich_table = RichTable(box=box.SIMPLE)
    for cn in table.column_names:
        rich_table.add_column(cn)

    num_split_rows = 1

    if rows_head is not None:

        if rows_head < 0:
            rows_head = 0

        if rows_head > table.num_rows:
            rows_head = table.num_rows
            rows_tail = None
            num_split_rows = 0

        if rows_tail is not None:
            if rows_head + rows_tail >= table.num_rows:  # type: ignore
                rows_head = table.num_rows
                rows_tail = None
                num_split_rows = 0
    else:
        num_split_rows = 0

    if rows_head is not None:
        head = table.slice(0, rows_head)
        num_rows = rows_head
    else:
        head = table
        num_rows = table.num_rows

    table_dict = head.to_pydict()
    for i in range(0, num_rows):
        row = []
        for cn in table.column_names:
            cell = table_dict[cn][i]
            cell_str = str(cell)
            if max_row_height and max_row_height > 0 and "\n" in cell_str:
                lines = cell_str.split("\n")
                if len(lines) > max_row_height:
                    if max_row_height == 1:
                        lines = lines[0:1]
                    else:
                        half = int(len(lines) / 2)
                        lines = lines[0:half] + [".."] + lines[-half:]
                cell_str = "\n".join(lines)

            if max_cell_length and max_cell_length > 0:
                lines = []
                for line in cell_str.split("\n"):
                    if len(line) > max_cell_length:
                        line = line[0:(max_cell_length)] + " ..."
                    else:
                        line = line
                    lines.append(line)
                cell_str = "\n".join(lines)

            row.append(cell_str)

        rich_table.add_row(*row)

    if num_split_rows:
        for i in range(0, num_split_rows):
            row = []
            for _ in table.column_names:
                row.append("...")
            rich_table.add_row(*row)

    if rows_head:
        if rows_tail is not None:
            if rows_tail < 0:
                rows_tail = 0

            tail = table.slice(table.num_rows - rows_tail)
            table_dict = tail.to_pydict()
            for i in range(0, num_rows):

                row = []
                for cn in table.column_names:
                    cell = table_dict[cn][i]
                    cell_str = str(cell)

                    if max_row_height and max_row_height > 0 and "\n" in cell_str:
                        lines = cell_str.split("\n")
                        if len(lines) > max_row_height:
                            if max_row_height == 1:
                                lines = lines[0:1]
                            else:
                                half = int(len(lines) / 2)
                                lines = lines[0:half] + [".."] + lines[-half:]
                        cell_str = "\n".join(lines)

                    if max_cell_length and max_cell_length > 0:
                        lines = []
                        for line in cell_str.split("\n"):

                            if len(line) > max_cell_length:
                                line = line[0:(max_cell_length)] + " ..."
                            else:
                                line = line
                            lines.append(line)
                        cell_str = "\n".join(lines)

                    row.append(cell_str)

                rich_table.add_row(*row)

    return rich_table