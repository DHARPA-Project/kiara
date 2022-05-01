# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import json
import orjson
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, root_validator
from rich import box
from rich.console import ConsoleRenderable, Group, RenderableType, RichCast
from rich.table import Table as RichTable
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Type,
)

from kiara.defaults import SpecialValue
from kiara.models.values.value import ORPHAN, Value, ValueMap
from kiara.utils import dict_from_cli_args, orjson_dumps

if TYPE_CHECKING:
    from pyarrow import Table as ArrowTable
    from sqlalchemy.engine import Engine

    from kiara.models.values.value_schema import ValueSchema


class RenderConfig(BaseModel):

    render_format: str = Field(description="The output format.", default="terminal")


class OutputDetails(BaseModel):
    @classmethod
    def from_data(cls, data: Any):

        if isinstance(data, str):
            if "=" in data:
                data = [data]
            else:
                data = [f"format={data}"]

        if isinstance(data, Iterable):
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
    config: Dict[str, Any] = Field(
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


# def pretty_print_table(
#     table: "Table",
#     rows_head: typing.Optional[int] = None,
#     rows_tail: typing.Optional[int] = None,
#     max_row_height: typing.Optional[int] = None,
#     max_cell_length: typing.Optional[int] = None,
# ) -> RenderableType:
#
#     rich_table = RichTable(box=box.SIMPLE)
#     for cn in table.column_names:
#         rich_table.add_column(cn)
#
#     num_split_rows = 2
#
#     if rows_head is not None:
#
#         if rows_head < 0:
#             rows_head = 0
#
#         if rows_head > table.num_rows:
#             rows_head = table.num_rows
#             rows_tail = None
#             num_split_rows = 0
#
#         if rows_tail is not None:
#             if rows_head + rows_tail >= table.num_rows:  # type: ignore
#                 rows_head = table.num_rows
#                 rows_tail = None
#                 num_split_rows = 0
#     else:
#         num_split_rows = 0
#
#     if rows_head is not None:
#         head = table.slice(0, rows_head)
#         num_rows = rows_head
#     else:
#         head = table
#         num_rows = table.num_rows
#
#     table_dict = head.to_pydict()
#     for i in range(0, num_rows):
#         row = []
#         for cn in table.column_names:
#             cell = table_dict[cn][i]
#             cell_str = str(cell)
#             if max_row_height and max_row_height > 0 and "\n" in cell_str:
#                 lines = cell_str.split("\n")
#                 if len(lines) > max_row_height:
#                     if max_row_height == 1:
#                         lines = lines[0:1]
#                     else:
#                         half = int(max_row_height / 2)
#                         lines = lines[0:half] + [".."] + lines[-half:]
#                 cell_str = "\n".join(lines)
#
#             if max_cell_length and max_cell_length > 0:
#                 lines = []
#                 for line in cell_str.split("\n"):
#                     if len(line) > max_cell_length:
#                         line = line[0:max_cell_length] + " ..."
#                     else:
#                         line = line
#                     lines.append(line)
#                 cell_str = "\n".join(lines)
#
#             row.append(cell_str)
#
#         rich_table.add_row(*row)
#
#     if num_split_rows:
#         for i in range(0, num_split_rows):
#             row = []
#             for _ in table.column_names:
#                 row.append("...")
#             rich_table.add_row(*row)
#
#     if rows_head:
#         if rows_tail is not None:
#             if rows_tail < 0:
#                 rows_tail = 0
#
#             tail = table.slice(table.num_rows - rows_tail)
#             table_dict = tail.to_pydict()
#             for i in range(0, num_rows):
#
#                 row = []
#                 for cn in table.column_names:
#
#                     cell = table_dict[cn][i]
#                     cell_str = str(cell)
#
#                     if max_row_height and max_row_height > 0 and "\n" in cell_str:
#                         lines = cell_str.split("\n")
#                         if len(lines) > max_row_height:
#                             if max_row_height == 1:
#                                 lines = lines[0:1]
#                             else:
#                                 half = int(len(lines) / 2)
#                                 lines = lines[0:half] + [".."] + lines[-half:]
#                         cell_str = "\n".join(lines)
#
#                     if max_cell_length and max_cell_length > 0:
#                         lines = []
#                         for line in cell_str.split("\n"):
#
#                             if len(line) > max_cell_length:
#                                 line = line[0:(max_cell_length)] + " ..."
#                             else:
#                                 line = line
#                             lines.append(line)
#                         cell_str = "\n".join(lines)
#
#                     row.append(cell_str)
#
#                 rich_table.add_row(*row)
#
#     return rich_table


class TabularWrap(ABC):
    def __init__(self):
        self._num_rows: Optional[int] = None
        self._column_names: Optional[Iterable[str]] = None

    @property
    def num_rows(self) -> int:
        if self._num_rows is None:
            self._num_rows = self.retrieve_number_of_rows()
        return self._num_rows

    @property
    def column_names(self) -> Iterable[str]:
        if self._column_names is None:
            self._column_names = self.retrieve_column_names()
        return self._column_names

    @abstractmethod
    def retrieve_column_names(self) -> Iterable[str]:
        pass

    @abstractmethod
    def retrieve_number_of_rows(self) -> int:
        pass

    @abstractmethod
    def slice(self, offset: int = 0, length: Optional[int] = None) -> "TabularWrap":
        pass

    @abstractmethod
    def to_pydict(self) -> Mapping:
        pass

    def pretty_print(
        self,
        rows_head: Optional[int] = None,
        rows_tail: Optional[int] = None,
        max_row_height: Optional[int] = None,
        max_cell_length: Optional[int] = None,
        show_table_header: bool = True,
    ) -> RenderableType:

        rich_table = RichTable(box=box.SIMPLE, show_header=show_table_header)
        for cn in self.retrieve_column_names():
            rich_table.add_column(cn)

        num_split_rows = 2

        if rows_head is not None:

            if rows_head < 0:
                rows_head = 0

            if rows_head > self.retrieve_number_of_rows():
                rows_head = self.retrieve_number_of_rows()
                rows_tail = None
                num_split_rows = 0

            if rows_tail is not None:
                if rows_head + rows_tail >= self.num_rows:  # type: ignore
                    rows_head = self.retrieve_number_of_rows()
                    rows_tail = None
                    num_split_rows = 0
        else:
            num_split_rows = 0

        if rows_head is not None:
            head = self.slice(0, rows_head)
            num_rows = rows_head
        else:
            head = self
            num_rows = self.retrieve_number_of_rows()

        table_dict = head.to_pydict()
        for i in range(0, num_rows):
            row = []
            for cn in self.retrieve_column_names():
                cell = table_dict[cn][i]
                cell_str = str(cell)
                if max_row_height and max_row_height > 0 and "\n" in cell_str:
                    lines = cell_str.split("\n")
                    if len(lines) > max_row_height:
                        if max_row_height == 1:
                            lines = lines[0:1]
                        else:
                            half = int(max_row_height / 2)
                            lines = lines[0:half] + [".."] + lines[-half:]
                    cell_str = "\n".join(lines)

                if max_cell_length and max_cell_length > 0:
                    lines = []
                    for line in cell_str.split("\n"):
                        if len(line) > max_cell_length:
                            line = line[0:max_cell_length] + " ..."
                        else:
                            line = line
                        lines.append(line)
                    cell_str = "\n".join(lines)

                row.append(cell_str)

            rich_table.add_row(*row)

        if num_split_rows:
            for i in range(0, num_split_rows):
                row = []
                for _ in self.retrieve_column_names():
                    row.append("...")
                rich_table.add_row(*row)

        if rows_head:
            if rows_tail is not None:
                if rows_tail < 0:
                    rows_tail = 0

                tail = self.slice(self.retrieve_number_of_rows() - rows_tail)
                table_dict = tail.to_pydict()
                for i in range(0, num_rows):

                    row = []
                    for cn in self.retrieve_column_names():

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


class ArrowTabularWrap(TabularWrap):
    def __init__(self, table: "ArrowTable"):
        self._table: "ArrowTable" = table
        super().__init__()

    def retrieve_column_names(self) -> Iterable[str]:
        return self._table.column_names

    def retrieve_number_of_rows(self) -> int:
        return self._table.num_rows

    def slice(self, offset: int = 0, length: Optional[int] = None):
        return self._table.slice(offset=offset, length=length)

    def to_pydict(self) -> Mapping:
        return self._table.to_pydict()


class DictTabularWrap(TabularWrap):
    def __init__(self, data: Mapping[str, Any]):

        self._data: Mapping[str, Any] = data

    def retrieve_number_of_rows(self) -> int:
        return len(self._data)

    def retrieve_column_names(self) -> Iterable[str]:
        return self._data.keys()

    def to_pydict(self) -> Mapping:
        return self._data

    def slice(self, offset: int = 0, length: Optional[int] = None) -> "TabularWrap":

        result = {}
        start = None
        end = None
        for cn in self._data.keys():
            if start is None:
                if offset > len(self._data):
                    return DictTabularWrap({cn: [] for cn in self._data.keys()})
                start = offset
                if not length:
                    end = len(self._data)
                else:
                    end = start + length
                    if end > len(self._data):
                        end = len(self._data)
            result[cn] = self._data[cn][start:end]
        return DictTabularWrap(result)


class SqliteTabularWrap(TabularWrap):
    def __init__(self, engine: "Engine", table_name: str):
        self._engine: Engine = engine
        self._table_name: str = table_name
        super().__init__()

    def retrieve_number_of_rows(self) -> int:

        from sqlalchemy import text

        with self._engine.connect() as con:
            result = con.execute(text(f"SELECT count(*) from {self._table_name}"))
            num_rows = result.fetchone()[0]

        return num_rows

    def retrieve_column_names(self) -> Iterable[str]:

        from sqlalchemy import inspect

        engine = self._engine
        inspector = inspect(engine)
        columns = inspector.get_columns(self._table_name)
        result = [column["name"] for column in columns]
        return result

    def slice(self, offset: int = 0, length: Optional[int] = None) -> "TabularWrap":

        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"
        if length:
            query = f"{query} LIMIT {length}"
        else:
            query = f"{query} LIMIT {self.num_rows}"
        if offset > 0:
            query = f"{query} OFFSET {offset}"
        with self._engine.connect() as con:
            result = con.execute(text(query))
            result_dict: Dict[str, List[Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return DictTabularWrap(result_dict)

    def to_pydict(self) -> Mapping:

        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"

        with self._engine.connect() as con:
            result = con.execute(text(query))
            result_dict: Dict[str, List[Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return result_dict


def create_table_from_base_model_cls(model_cls: Type[BaseModel]):

    table = RichTable(box=box.SIMPLE, show_lines=True)
    table.add_column("Field")
    table.add_column("Type")
    table.add_column("Description")
    table.add_column("Required")
    table.add_column("Default")

    props = model_cls.schema().get("properties", {})

    for field_name, field in sorted(model_cls.__fields__.items()):
        row = [field_name]
        p = props.get(field_name, None)
        p_type = None
        if p is not None:
            p_type = p.get("type", None)
            # TODO: check 'anyOf' keys

        if p_type is None:
            p_type = "-- check source --"
        row.append(p_type)
        desc = p.get("description", "")
        row.append(desc)
        row.append("yes" if field.required else "no")
        default = field.default
        if callable(default):
            default = default()

        if default is None:
            default = ""
        else:
            try:
                default = json.dumps(default, indent=2)
            except Exception:
                default = str(default)
        row.append(default)
        table.add_row(*row)

    return table


# def create_table_from_config_class(
#     config_cls: typing.Type["KiaraModuleConfig"],
#     remove_pipeline_config: bool = False,
# ) -> Table:
#
#     table = Table(box=box.HORIZONTALS, show_header=False)
#     table.add_column("Field name", style="i")
#     table.add_column("Type")
#     table.add_column("Description")
#     flat_models = get_flat_models_from_model(config_cls)
#     model_name_map = get_model_name_map(flat_models)
#     m_schema, _, _ = model_process_schema(config_cls, model_name_map=model_name_map)
#     fields = m_schema["properties"]
#
#     for alias, details in fields.items():
#         if remove_pipeline_config and alias in [
#             "steps",
#             "input_aliases",
#             "output_aliases",
#             "doc",
#         ]:
#             continue
#
#         type_str = "-- n/a --"
#         if "type" in details.keys():
#             type_str = details["type"]
#         table.add_row(alias, type_str, details.get("description", "-- n/a --"))
#
#     return table


def create_table_from_field_schemas(
    _add_default: bool = True,
    _add_required: bool = True,
    _show_header: bool = False,
    _constants: Optional[Mapping[str, Any]] = None,
    **fields: "ValueSchema",
) -> RichTable:

    table = RichTable(box=box.SIMPLE, show_header=_show_header)
    table.add_column("field name", style="i")
    table.add_column("type")
    table.add_column("description")

    if _add_required:
        table.add_column("Required")
    if _add_default:
        if _constants:
            table.add_column("Default / Constant")
        else:
            table.add_column("Default")

    for field_name, schema in fields.items():

        row: List[RenderableType] = [field_name, schema.type, schema.doc]

        if _add_required:
            req = schema.is_required()
            if not req:
                req_str = "no"
            else:
                if schema.default in [
                    None,
                    SpecialValue.NO_VALUE,
                    SpecialValue.NOT_SET,
                ]:
                    req_str = "[b]yes[b]"
                else:
                    req_str = "no"
            row.append(req_str)

        if _add_default:
            if _constants and field_name in _constants.keys():
                d = f"[b]{_constants[field_name]}[/b] (constant)"
            else:
                if schema.default in [
                    None,
                    SpecialValue.NO_VALUE,
                    SpecialValue.NOT_SET,
                ]:
                    d = "-- no default --"
                else:
                    d = str(schema.default)
            row.append(d)

        table.add_row(*row)

    return table


def create_value_map_status_renderable(
    inputs: ValueMap, render_config: Optional[Mapping[str, Any]] = None
) -> RichTable:

    if render_config is None:
        render_config = {}

    show_required: bool = render_config.get("show_required", True)

    table = RichTable(box=box.SIMPLE, show_header=True)
    table.add_column("field name", style="i")
    table.add_column("status", style="b")
    table.add_column("type")
    table.add_column("description")

    if show_required:
        table.add_column("required")

    invalid = inputs.check_invalid()

    for field_name, value in inputs.items():

        row: List[RenderableType] = [field_name]

        if field_name in invalid.keys():
            row.append(f"[red]{invalid[field_name]}[/red]")
        else:
            row.append("[green]valid[/green]")

        row.extend([value.value_schema.type, value.value_schema.doc.description])

        if show_required:
            req = value.value_schema.is_required()
            if not req:
                req_str = "no"
            else:
                if value.value_schema.default in [
                    None,
                    SpecialValue.NO_VALUE,
                    SpecialValue.NOT_SET,
                ]:
                    req_str = "[b]yes[b]"
                else:
                    req_str = "no"
            row.append(req_str)

        table.add_row(*row)

    return table


def create_table_from_model_object(
    model: BaseModel,
    render_config: Optional[Mapping[str, Any]] = None,
    exclude_fields: Optional[Set[str]] = None,
):

    model_cls = model.__class__

    table = RichTable(box=box.SIMPLE, show_lines=True)
    table.add_column("Field")
    table.add_column("Type")
    table.add_column("Value")
    table.add_column("Description")

    props = model_cls.schema().get("properties", {})

    for field_name, field in sorted(model_cls.__fields__.items()):
        if exclude_fields and field_name in exclude_fields:
            continue
        row = [field_name]

        p = props.get(field_name, None)
        p_type = None
        if p is not None:
            p_type = p.get("type", None)
            # TODO: check 'anyOf' keys

        if p_type is None:
            p_type = "-- check source --"
        row.append(p_type)

        data = getattr(model, field_name)
        row.append(extract_renderable(data, render_config=render_config))

        desc = p.get("description", "")
        row.append(desc)
        table.add_row(*row)

    return table


def extract_renderable(item: Any, render_config: Optional[Mapping[str, Any]] = None):
    """Try to automatically find and extract or create an object that is renderable by the 'rich' library."""

    if render_config is None:
        render_config = {}
    else:
        render_config = dict(render_config)

    inline_models_as_json = render_config.setdefault("inline_models_as_json", True)

    if hasattr(item, "create_renderable"):
        return item.create_renderable(**render_config)
    elif isinstance(item, (ConsoleRenderable, RichCast, str)):
        return item
    elif isinstance(item, BaseModel) and not inline_models_as_json:
        return create_table_from_model_object(item)
    elif isinstance(item, BaseModel):
        return item.json(indent=2)
    elif isinstance(item, Mapping) and not inline_models_as_json:
        table = RichTable(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for k, v in item.items():
            table.add_row(k, extract_renderable(v, render_config=render_config))
        return table
    elif isinstance(item, Mapping):
        result = {}
        for k, v in item.items():
            if isinstance(v, BaseModel):
                v = v.dict()
            result[k] = v
        return orjson_dumps(
            result, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS
        )
    elif isinstance(item, Iterable):
        _all = []
        for i in item:
            _all.append(extract_renderable(i))
        rg = Group(*_all)
        return rg
    else:
        return str(item)


def create_renderable_from_values(
    values: Mapping[str, "Value"], config: Optional[Mapping[str, Any]] = None
) -> RenderableType:
    """Create a renderable for this module configuration."""

    if config is None:
        config = {}

    render_format = config.get("render_format", "terminal")
    if render_format not in ["terminal"]:
        raise Exception(f"Invalid render format: {render_format}")

    show_pedigree = config.get("show_pedigree", False)
    show_data = config.get("show_data", False)
    show_hash = config.get("show_hash", True)
    # show_load_config = config.get("show_load_config", False)

    table = RichTable(show_lines=True, box=box.MINIMAL_DOUBLE_HEAD)
    table.add_column("value_id", "i")
    table.add_column("data_type")
    table.add_column("size")
    if show_hash:
        table.add_column("hash")
    if show_pedigree:
        table.add_column("pedigree")
    if show_data:
        table.add_column("data")

    for id, value in sorted(values.items(), key=lambda item: item[1].value_schema.type):
        row: List[RenderableType] = [id, value.value_schema.type, str(value.value_size)]
        if show_hash:
            row.append(str(value.value_hash))
        if show_pedigree:
            if value.pedigree == ORPHAN:
                pedigree = "-- n/a --"
            else:
                pedigree = value.pedigree.json(option=orjson.OPT_INDENT_2)
            row.append(pedigree)
        if show_data:
            data = value._data_registry.render_data(
                value_id=value.value_id, target_type="terminal_renderable", **config
            )
            row.append(data)
        # if show_load_config:
        #     load_config = value.retrieve_load_config()
        #     if load_config is None:
        #         load_config_str: RenderableType = "-- not stored (yet) --"
        #     else:
        #         load_config_str = load_config.create_renderable()
        #     row.append(load_config_str)
        table.add_row(*row)

    return table
