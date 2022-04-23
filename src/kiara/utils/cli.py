# -*- coding: utf-8 -*-
import orjson
import rich_click as click
from click import Command, Option, Parameter
from enum import Enum
from pydantic import BaseModel
from rich.console import Group
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from typing import Any, Callable, Mapping, Optional, TypeVar, Union

from kiara.interfaces import get_console

# ======================================================================================================================
# click helper methods
from kiara.utils import orjson_dumps
from kiara.utils.output import extract_renderable

F = TypeVar("F", bound=Callable[..., Any])
FC = TypeVar("FC", bound=Union[Callable[..., Any], Command])


def _param_memo(f: FC, param: Parameter) -> None:
    if isinstance(f, Command):
        f.params.append(param)
    else:
        if not hasattr(f, "__click_params__"):
            f.__click_params__ = []  # type: ignore

        f.__click_params__.append(param)  # type: ignore


# ======================================================================================================================
def terminal_print(
    msg: Any = None,
    in_panel: Optional[str] = None,
    rich_config: Optional[Mapping[str, Any]] = None,
    empty_line_before: bool = False,
    **config: Any,
) -> None:

    if msg is None:
        msg = ""
    console = get_console()

    msg = extract_renderable(msg, render_config=config)
    # if hasattr(msg, "create_renderable"):
    #     msg = msg.create_renderable(**config)  # type: ignore

    if in_panel is not None:
        msg = Panel(msg, title_align="left", title=in_panel)

    if empty_line_before:
        console.print()
    if rich_config:
        console.print(msg, **rich_config)
    else:
        console.print(msg)


class OutputFormat(Enum):
    @classmethod
    def as_dict(cls):
        return {i.name: i.value for i in cls}

    @classmethod
    def keys_as_list(cls):
        return cls._member_names_

    @classmethod
    def values_as_list(cls):
        return [i.value for i in cls]

    TERMINAL = "terminal"
    HTML = "html"
    JSON = "json"
    JSON_INCL_SCHEMA = "json-incl-schema"
    JSON_SCHEMA = "json-schema"


def output_format_option(*param_decls: str) -> Callable[[FC], FC]:
    """Attaches an option to the command.  All positional arguments are
    passed as parameter declarations to :class:`Option`; all keyword
    arguments are forwarded unchanged (except ``cls``).
    This is equivalent to creating an :class:`Option` instance manually
    and attaching it to the :attr:`Command.params` list.

    :param cls: the option class to instantiate.  This defaults to
                :class:`Option`.
    """

    if not param_decls:
        param_decls = ("--format", "-f")

    attrs = {
        "help": "The output format. Defaults to 'terminal'.",
        "type": click.Choice(OutputFormat.values_as_list()),
    }

    def decorator(f: FC) -> FC:
        # Issue 926, copy attrs, so pre-defined options can re-use the same cls=
        option_attrs = attrs.copy()
        OptionClass = option_attrs.pop("cls", None) or Option
        _param_memo(f, OptionClass(param_decls, **option_attrs))  # type: ignore
        return f

    return decorator


def render_json_str(model: BaseModel):

    try:
        json_str = model.json(option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS)
    except TypeError:
        json_str = model.json(indent=2)

    return json_str


def render_json_schema_str(model: BaseModel):

    try:
        json_str = model.schema_json(option=orjson.OPT_INDENT_2)
    except TypeError:
        json_str = model.schema_json(indent=2)

    return json_str


def terminal_print_model(
    *models: BaseModel,
    format: Union[None, OutputFormat, str] = None,
    empty_line_before: Optional[bool] = None,
    in_panel: Optional[str] = None,
    **render_config: Any,
):

    if format is None:
        format = OutputFormat.TERMINAL

    if isinstance(format, str):
        format = OutputFormat(format)

    if empty_line_before is None:
        if format == OutputFormat.TERMINAL:
            empty_line_before = True
        else:
            empty_line_before = False

    if format == OutputFormat.TERMINAL:
        if len(models) == 1:
            terminal_print(
                models[0],
                in_panel=in_panel,
                empty_line_before=empty_line_before,
                **render_config,
            )
        else:
            rg = []
            for model in models[0:-1]:
                renderable = extract_renderable(model, render_config)
                rg.append(renderable)
                rg.append(Rule(style="b"))
            last = extract_renderable(models[-1], render_config)
            rg.append(last)
            group = Group(*rg)
            terminal_print(group, in_panel=in_panel, **render_config)
    elif format == OutputFormat.JSON:
        if len(models) == 1:
            json_str = render_json_str(models[0])
            syntax = Syntax(json_str, "json", background_color="default")
            terminal_print(
                syntax,
                empty_line_before=empty_line_before,
                rich_config={"soft_wrap": True},
            )
        else:
            json_strs = []
            for model in models:
                json_str = render_json_str(model)
                json_strs.append(json_str)

            json_str_full = "[" + ",\n".join(json_strs) + "]"
            syntax = Syntax(json_str_full, "json", background_color="default")
            terminal_print(
                syntax,
                empty_line_before=empty_line_before,
                rich_config={"soft_wrap": True},
            )

    elif format == OutputFormat.JSON_SCHEMA:
        if len(models) == 1:
            syntax = Syntax(
                models[0].schema_json(option=orjson.OPT_INDENT_2),
                "json",
                background_color="default",
            )
            terminal_print(
                syntax,
                empty_line_before=empty_line_before,
                rich_config={"soft_wrap": True},
            )
        else:
            json_strs = []
            for model in models:
                json_strs.append(render_json_schema_str(model))
            json_str_full = "[" + ",\n".join(json_strs) + "]"
            syntax = Syntax(json_str_full, "json", background_color="default")
            terminal_print(
                syntax,
                empty_line_before=empty_line_before,
                rich_config={"soft_wrap": True},
            )
    elif format == OutputFormat.JSON_INCL_SCHEMA:
        if len(models) == 1:
            data = models[0].dict()
            schema = models[0].schema()
            all = {"data": data, "schema": schema}
            json_str = orjson_dumps(all, option=orjson.OPT_INDENT_2)
            syntax = Syntax(json_str, "json", background_color="default")
            terminal_print(
                syntax,
                empty_line_before=empty_line_before,
                rich_config={"soft_wrap": True},
            )
        else:
            all_data = []
            for model in models:
                data = model.dict()
                schema = model.schema()
                all_data.append({"data": data, "schema": schema})
            json_str = orjson_dumps(all_data, option=orjson.OPT_INDENT_2)
            # print(json_str)
            syntax = Syntax(json_str, "json", background_color="default")
            terminal_print(
                syntax,
                empty_line_before=empty_line_before,
                rich_config={"soft_wrap": True},
            )

    elif format == OutputFormat.HTML:

        all_html = ""
        for model in models:
            if hasattr(model, "create_html"):
                html = model.create_html()  # type: ignore
                all_html = f"{all_html}\n{html}"
            else:
                raise NotImplementedError()

        syntax = Syntax(all_html, "html", background_color="default")
        terminal_print(
            syntax, empty_line_before=empty_line_before, rich_config={"soft_wrap": True}
        )
