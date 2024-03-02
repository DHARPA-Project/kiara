# -*- coding: utf-8 -*-
import json
import os
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Mapping, TypeVar, Union

import rich_click as click
from click import Command, Context, Option, Parameter, option
from rich import box
from rich.box import Box
from rich.console import ConsoleRenderable, Group, RichCast
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax

from kiara.defaults import KIARA_MAIN_CONFIG_FILE, kiara_app_dirs

# ======================================================================================================================
# click helper methods
from kiara.utils import logger

if TYPE_CHECKING:
    from pydantic import BaseModel

F = TypeVar("F", bound=Callable[..., Any])
FC = TypeVar("FC", bound=Union[Callable[..., Any], Command])


def _param_memo(f: Union[Callable[..., Any], Command], param: Parameter) -> None:
    if isinstance(f, Command):
        f.params.append(param)
    else:
        if not hasattr(f, "__click_params__"):
            f.__click_params__ = []  # type: ignore

        f.__click_params__.append(param)  # type: ignore


HORIZONTALS_NO_TO_AND_BOTTOM: Box = Box(
    """\
    
    
 ── 
    
 ── 
 ── 
    
    
"""
)


# ======================================================================================================================
def terminal_print(
    msg: Union[Any, None] = None,
    in_panel: Union[str, None] = None,
    rich_config: Union[Mapping[str, Any], None] = None,
    empty_line_before: bool = False,
    **config: Any,
) -> None:

    from kiara.interfaces import get_console
    from kiara.utils.output import extract_renderable

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


def is_rich_renderable(item: Any):
    return isinstance(item, (ConsoleRenderable, RichCast, str))


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
    """
    Attaches an option to the command.  All positional arguments are
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


def render_json_str(model: "BaseModel"):

    # import orjson

    # TODO: pydantic refactor
    json_str = model.model_dump_json(indent=2)

    # try:
    #     json_str = model.json(option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS)
    # except TypeError:
    #     json_str = model.model_dump_json(indent=2)

    return json_str


def render_json_schema_str(model: "BaseModel"):

    #
    # try:
    #     json_str = model.schema_json(option=orjson.OPT_INDENT_2)
    # except TypeError:
    #     json_str = model.schema_json(indent=2)

    import orjson

    from kiara.utils.json import orjson_dumps

    schema = model.model_json_schema()
    json_str = orjson_dumps(schema, option=orjson.OPT_INDENT_2)

    return json_str


def terminal_print_model(
    *models: "BaseModel",
    format: Union[None, OutputFormat, str] = None,
    empty_line_before: Union[bool, None] = None,
    in_panel: Union[str, None] = None,
    **render_config: Any,
):

    import orjson

    from kiara.utils.json import orjson_dumps
    from kiara.utils.output import extract_renderable

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
            if not models:
                return
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
            _schema = models[0].model_json_schema()
            schema_str = orjson_dumps(_schema, option=orjson.OPT_INDENT_2)
            syntax = Syntax(
                schema_str,
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
            data = models[0].model_dump()
            schema = models[0].model_json_schema()
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
                data = model.model_dump()
                schema = model.model_json_schema()
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


def dict_from_cli_args(
    *args: str, list_keys: Union[Iterable[str], None] = None
) -> Dict[str, Any]:

    if not args:
        return {}

    config: Dict[str, Any] = {}
    for arg in args:
        if "=" in arg:
            key, value = arg.split("=", maxsplit=1)
            if value.startswith("alias:"):
                if key in config.keys():
                    raise Exception(f"alias key already set: {key} : {value}")
                config[key] = value
                continue

            try:
                _v = json.loads(value)
            except Exception:
                _v = value
            part_config = {key: _v}
        elif os.path.isfile(os.path.realpath(os.path.expanduser(arg))):
            path = os.path.realpath(os.path.expanduser(arg))
            from kiara.utils.files import get_data_from_file

            part_config = get_data_from_file(path)
            assert isinstance(part_config, Mapping)
        else:
            try:
                part_config = json.loads(arg)
                assert isinstance(part_config, Mapping)
            except Exception:
                raise Exception(f"Could not parse argument into data: {arg}")

        if list_keys is None:
            list_keys = []

        for k, v in part_config.items():
            if k in list_keys:
                config.setdefault(k, []).append(v)
            else:
                if k in config.keys():
                    logger.warning("duplicate.key", old_value=k, new_value=v)
                config[k] = v

    return config


def kiara_version_option(
    **kwargs: Any,
) -> Callable[[FC], FC]:
    """Add a ``--version`` option which immediately prints the version
    number of kiara and all installed plugins.
    """

    def callback(ctx: Context, param: Parameter, value: bool) -> None:

        from rich.table import Table

        if not value or ctx.resilient_parsing:
            return

        from kiara.models.runtime_environment.python import (
            PythonRuntimeEnvironment,
        )
        from kiara.registries.environment import EnvironmentRegistry

        registry = EnvironmentRegistry.instance()
        python_env: PythonRuntimeEnvironment = registry.environments["python"]  # type: ignore

        kiara_version = None
        plugins = {}
        for pkg in python_env.packages:
            if pkg.name == "kiara":
                kiara_version = pkg.version
            elif pkg.name.startswith("kiara"):
                plugins[pkg.name] = pkg.version

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("package")
        table.add_column("version", style="i")

        table.add_row("kiara", kiara_version)
        table.add_row("", "")
        for name in sorted(plugins.keys()):
            table.add_row(name, plugins[name])

        table.add_row("", "")
        table.add_row("python", python_env.python_version)
        terminal_print()
        terminal_print(table, in_panel="Version information")

        ctx.exit()

    param_decls = ("--version", "-v")

    kwargs.setdefault("is_flag", True)
    kwargs.setdefault("expose_value", False)
    kwargs.setdefault("is_eager", True)
    kwargs.setdefault(
        "help", ("Show the version of kiara and installed plugins, then exit.")
    )
    kwargs["callback"] = callback
    result: Callable[[FC], FC] = option(*param_decls, **kwargs)
    return result


def kiara_runtime_info_option(
    **kwargs: Any,
) -> Callable[[FC], FC]:
    """Add a ``--runtime-info`` option which immediately prints information about the current application environment."""

    def callback(ctx: Context, param: Parameter, value: bool) -> None:

        from rich.table import Table

        if not value or ctx.resilient_parsing:
            return

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key")
        table.add_column("value", style="i")
        config_file = KIARA_MAIN_CONFIG_FILE
        table.add_row("config file", config_file)
        table.add_row("", "")
        data_path = kiara_app_dirs.user_data_dir
        table.add_row("data path", data_path)
        table.add_row("", "")
        cache_path = kiara_app_dirs.user_cache_dir
        table.add_row("cache path", cache_path)
        table.add_row("", "")

        terminal_print()
        terminal_print(table, in_panel="Application runtime information")

        ctx.exit()

    param_decls = ("--runtime-info", "-ri")

    kwargs.setdefault("is_flag", True)
    kwargs.setdefault("expose_value", False)
    kwargs.setdefault("is_eager", True)
    kwargs.setdefault("help", ("Show current environment information, then exit."))
    kwargs["callback"] = callback
    result: Callable[[FC], FC] = option(*param_decls, **kwargs)
    return result
