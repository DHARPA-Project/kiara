# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import json
import orjson
import structlog
from abc import ABC, abstractmethod
from enum import Enum
from pydantic import BaseModel, Field, root_validator
from rich import box
from rich.console import ConsoleRenderable, Group, RenderableType, RichCast
from rich.table import Table as RichTable
from rich.tree import Tree
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Set,
    Type,
    Union,
)

from kiara.defaults import SpecialValue
from kiara.models.values.value import ORPHAN, Value, ValueMap
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from pyarrow import Table as ArrowTable

    from kiara.models.events.pipeline import PipelineState
    from kiara.models.module.pipeline import PipelineStructure
    from kiara.models.values.value_schema import ValueSchema


log = structlog.getLogger()


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
            from kiara.utils.cli import dict_from_cli_args

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
    def _set_defaults(cls, values) -> Dict[str, Any]:

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


class TabularWrap(ABC):
    def __init__(self) -> None:
        self._num_rows: Union[int, None] = None
        self._column_names: Union[Iterable[str], None] = None
        self._force_single_line: bool = True

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
    def slice(self, offset: int = 0, length: Union[int, None] = None) -> "TabularWrap":
        pass

    @abstractmethod
    def to_pydict(self) -> Mapping:
        pass

    def as_string(
        self,
        rows_head: Union[int, None] = None,
        rows_tail: Union[int, None] = None,
        max_row_height: Union[int, None] = None,
        max_cell_length: Union[int, None] = None,
    ):

        table_str = ""
        for cn in self.column_names:
            table_str = f"{table_str}{cn}\t"
        table_str = f"{table_str}\n"

        for data in self.prepare_table_data(
            return_column_names=False,
            rows_head=rows_head,
            rows_tail=rows_tail,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        ):
            for cell in data:
                table_str = f"{table_str}{cell}\t"
            table_str = f"{table_str}\n"

        return table_str

    def as_html(
        self,
        rows_head: Union[int, None] = None,
        rows_tail: Union[int, None] = None,
        max_row_height: Union[int, None] = None,
        max_cell_length: Union[int, None] = None,
    ) -> str:

        table_str = "<table><tr>"
        for cn in self.column_names:
            table_str = f"{table_str}<th>{cn}</th>"
        table_str = f"{table_str}</tr>"

        for data in self.prepare_table_data(
            return_column_names=False,
            rows_head=rows_head,
            rows_tail=rows_tail,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        ):
            table_str = f"{table_str}<tr>"
            for cell in data:
                table_str = f"{table_str}<td>{cell}</td>"
            table_str = f"{table_str}</tr>"
        table_str = f"{table_str}</table>"
        return table_str

    def as_terminal_renderable(
        self,
        rows_head: Union[int, None] = None,
        rows_tail: Union[int, None] = None,
        max_row_height: Union[int, None] = None,
        max_cell_length: Union[int, None] = None,
        show_table_header: bool = True,
    ) -> RichTable:

        rich_table = RichTable(show_header=show_table_header, box=box.SIMPLE)
        if max_row_height == 1:
            overflow = "ignore"
        else:
            overflow = "ellipsis"

        for cn in self.column_names:
            rich_table.add_column(cn, overflow=overflow)  # type: ignore

        data = self.prepare_table_data(
            return_column_names=False,
            rows_head=rows_head,
            rows_tail=rows_tail,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )

        for row in data:
            rich_table.add_row(*row)

        return rich_table

    def prepare_table_data(
        self,
        return_column_names: bool = False,
        rows_head: Union[int, None] = None,
        rows_tail: Union[int, None] = None,
        max_row_height: Union[int, None] = None,
        max_cell_length: Union[int, None] = None,
    ) -> Iterator[Iterable[Any]]:

        if return_column_names:
            yield self.column_names

        num_split_rows = 2

        if rows_head is not None:

            if rows_head < 0:
                rows_head = 0

            if rows_head > self.num_rows:
                rows_head = self.num_rows
                rows_tail = None
                num_split_rows = 0

            if rows_tail is not None:
                if rows_head + rows_tail >= self.num_rows:  # type: ignore
                    rows_head = self.num_rows
                    rows_tail = None
                    num_split_rows = 0
        else:
            num_split_rows = 0

        if rows_head is not None:
            head = self.slice(0, rows_head)
            num_rows = rows_head
        else:
            head = self
            num_rows = self.num_rows

        table_dict = head.to_pydict()
        for i in range(0, num_rows):
            row = []
            for cn in self.column_names:
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

            yield row

        if num_split_rows:
            for i in range(0, num_split_rows):
                row = []
                for _ in self.column_names:
                    row.append("...")
                yield row

        if rows_head:
            if rows_tail is not None:
                if rows_tail < 0:
                    rows_tail = 0

                tail = self.slice(self.num_rows - rows_tail)
                table_dict = tail.to_pydict()
                for i in range(0, num_rows):

                    row = []
                    for cn in self.column_names:

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

                    yield row

        return


class ArrowTabularWrap(TabularWrap):
    def __init__(self, table: "ArrowTable"):
        self._table: "ArrowTable" = table
        super().__init__()

    def retrieve_column_names(self) -> Iterable[str]:
        return self._table.column_names

    def retrieve_number_of_rows(self) -> int:
        return self._table.num_rows

    def slice(self, offset: int = 0, length: Union[int, None] = None):
        return self._table.slice(offset=offset, length=length)

    def to_pydict(self) -> Mapping:
        return self._table.to_pydict()


class DictTabularWrap(TabularWrap):
    def __init__(self, data: Mapping[str, List[Any]]):

        self._data: Mapping[str, List[Any]] = data
        # TODO: assert all rows are equal length
        super().__init__()

    def retrieve_number_of_rows(self) -> int:
        key = next(iter(self._data.keys()))
        return len(self._data[key])

    def retrieve_column_names(self) -> Iterable[str]:
        return self._data.keys()

    def to_pydict(self) -> Mapping[str, List[Any]]:
        return self._data

    def slice(self, offset: int = 0, length: Union[int, None] = None) -> "TabularWrap":

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


def create_dict_from_field_schemas(
    fields: Mapping[str, "ValueSchema"],
    _add_default: bool = True,
    _add_required: bool = True,
    _show_header: bool = False,
    _constants: Union[Mapping[str, Any], None] = None,
    _doc_to_string: bool = True,
) -> Mapping[str, List[Any]]:

    table: Dict[str, List[Any]] = {}
    table["field_name"] = []
    table["data_type"] = []
    table["description"] = []

    if _add_required:
        table["required"] = []
    if _add_default:
        table["default"] = []

    for field_name, schema in fields.items():

        table["field_name"].append(field_name)
        table["data_type"].append(schema.type)
        if _doc_to_string:
            table["description"].append(schema.doc.full_doc)
        else:
            table["description"].append(schema.doc)

        if _add_required:
            req = schema.is_required()
            table["required"].append(req)

        if _add_default:
            if _constants and field_name in _constants.keys():
                d = f"{_constants[field_name]} (constant)"
            else:
                if schema.default in [
                    None,
                    SpecialValue.NO_VALUE,
                    SpecialValue.NOT_SET,
                ]:
                    d = "-- no default --"
                else:
                    d = str(schema.default)
            table["default"].append(d)

    return table


def create_table_from_field_schemas(
    fields: Mapping[str, "ValueSchema"],
    _add_default: bool = True,
    _add_required: bool = True,
    _show_header: bool = False,
    _constants: Union[Mapping[str, Any], None] = None,
) -> RichTable:

    table = RichTable(box=box.SIMPLE, show_header=_show_header)
    table.add_column("field name", style="i", overflow="fold")
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
    inputs: ValueMap,
    render_config: Union[Mapping[str, Any], None] = None,
    fields: Union[None, Iterable[str]] = None,
) -> RichTable:

    if render_config is None:
        render_config = {}

    show_description: bool = render_config.get("show_description", True)
    show_type: bool = render_config.get("show_type", True)
    show_required: bool = render_config.get("show_required", True)
    show_default: bool = render_config.get("show_default", True)
    show_value_ids: bool = render_config.get("show_value_ids", False)

    table = RichTable(box=box.SIMPLE, show_header=True)
    table.add_column("field name", style="i")
    table.add_column("status", style="b")
    if show_type:
        table.add_column("type")
    if show_description:
        table.add_column("description")

    if show_required:
        table.add_column("required")

    if show_default:
        table.add_column("default")

    if show_value_ids:
        table.add_column("value id", overflow="fold")

    invalid = inputs.check_invalid()

    if fields:
        field_order = fields
    else:
        field_order = sorted(inputs.keys())

    for field_name in field_order:

        value = inputs.get(field_name, None)
        if value is None:
            log.debug(
                "ignore.field", field_name=field_name, available_fields=inputs.keys()
            )
            continue

        row: List[RenderableType] = [field_name]

        if field_name in invalid.keys():
            row.append(f"[red]{invalid[field_name]}[/red]")
        else:
            row.append("[green]valid[/green]")

        value_schema = inputs.values_schema[field_name]

        if show_type:
            row.append(value_schema.type)

        if show_description:
            row.append(value_schema.doc.description)

        if show_required:
            req = value_schema.is_required()
            if not req:
                req_str = "no"
            else:
                if value_schema.default in [
                    None,
                    SpecialValue.NO_VALUE,
                    SpecialValue.NOT_SET,
                ]:
                    req_str = "[b]yes[b]"
                else:
                    req_str = "no"
            row.append(req_str)

        if show_default:
            default = value_schema.default
            if callable(default):
                default_val = default()
            else:
                default_val = default

            if default_val in [None, SpecialValue.NOT_SET, SpecialValue.NO_VALUE]:
                default_str = ""
            else:
                default_str = str(default_val)

            row.append(default_str)

        if show_value_ids:
            row.append(str(inputs.get_value_obj(field_name=field_name).value_id))

        table.add_row(*row)

    return table


def create_table_from_model_object(
    model: BaseModel,
    render_config: Union[Mapping[str, Any], None] = None,
    exclude_fields: Union[Set[str], None] = None,
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
        row: List[RenderableType] = [field_name]

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


def extract_renderable(
    item: Any, render_config: Union[Mapping[str, Any], None] = None
) -> RenderableType:
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
    elif isinstance(item, Enum):
        return item.value
    else:
        return str(item)


def create_renderable_from_values(
    values: Mapping[str, "Value"], config: Union[Mapping[str, Any], None] = None
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
            data = value._data_registry.pretty_print_data(
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


def create_pipeline_steps_tree(
    pipeline_structure: "PipelineStructure", pipeline_details: "PipelineState"
) -> Tree:

    from kiara.models.module.pipeline import StepStatus

    steps = Tree("steps")

    for idx, stage in enumerate(pipeline_structure.processing_stages, start=1):
        stage_node = steps.add(f"stage: [i]{idx}[/i]")
        for step_id in sorted(stage):
            step_node = stage_node.add(f"step: [i]{step_id}[/i]")
            step_details = pipeline_details.step_states[step_id]
            status = step_details.status
            if status is StepStatus.INPUTS_READY:
                step_node.add("status: [yellow]inputs ready[/yellow]")
            elif status is StepStatus.RESULTS_READY:
                step_node.add("status: [green]results ready[/green]")
            else:
                invalid_node = step_node.add("status: [red]inputs invalid[/red]")
                invalid = step_details.invalid_details
                for k, v in invalid.items():
                    invalid_node.add(f"[i]{k}[/i]: {v}")

    return steps


def create_recursive_table_from_model_object(
    model: BaseModel,
    render_config: Union[Mapping[str, Any], None] = None,
):

    if render_config is None:
        render_config = {}

    show_lines = render_config.get("show_lines", True)
    show_header = render_config.get("show_header", True)
    model_cls = model.__class__

    table = RichTable(box=box.SIMPLE, show_lines=show_lines, show_header=show_header)
    table.add_column("Field")
    table.add_column("Value")

    props = model_cls.schema().get("properties", {})

    for field_name in sorted(model_cls.__fields__.keys()):

        data = getattr(model, field_name)
        p = props.get(field_name, None)
        p_type = None
        if p is not None:
            p_type = p.get("type", None)
            # TODO: check 'anyOf' keys

        if p_type is not None:
            p_type = f"[i]{p_type}[/i]"

        desc = p.get("description", None)

        if not isinstance(data, BaseModel):
            data_renderable = extract_renderable(data, render_config=render_config)
            sub_model = None
        else:
            sub_model = create_recursive_table_from_model_object(
                data, render_config={"show_lines": True, "show_header": False}
            )
            data_renderable = None

        group = []

        if data_renderable:
            group.append(data_renderable)
            group.append("")
        if desc:
            group.append(f"[i]{desc}[/i]")

        if sub_model:
            group.append(sub_model)

        if p_type:
            field_name = f"[b i]{field_name}[/b i] ([i]{p_type}[/i])"
        else:
            field_name = f"[b i]{field_name}[/b i]"
        table.add_row(field_name, Group(*group))

    return table
