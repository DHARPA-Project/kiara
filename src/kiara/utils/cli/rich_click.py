# -*- coding: utf-8 -*-
import click
import inspect
import textwrap
from rich import box
from rich.align import Align
from rich.console import Group, RenderableType
from rich.highlighter import RegexHighlighter
from rich.markdown import Markdown
from rich.padding import Padding
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Support rich <= 10.6.0
from rich_click.rich_click import (
    ALIGN_COMMANDS_PANEL,
    ALIGN_OPTIONS_PANEL,
    ARGUMENTS_PANEL_TITLE,
    COMMAND_GROUPS,
    COMMANDS_PANEL_TITLE,
    FOOTER_TEXT,
    GROUP_ARGUMENTS_OPTIONS,
    HEADER_TEXT,
    MAX_WIDTH,
    OPTION_GROUPS,
    OPTIONS_PANEL_TITLE,
    RANGE_STRING,
    REQUIRED_SHORT_STRING,
    SHOW_ARGUMENTS,
    SHOW_METAVARS_COLUMN,
    STYLE_COMMANDS_PANEL_BORDER,
    STYLE_COMMANDS_TABLE_BORDER_STYLE,
    STYLE_COMMANDS_TABLE_BOX,
    STYLE_COMMANDS_TABLE_LEADING,
    STYLE_COMMANDS_TABLE_PAD_EDGE,
    STYLE_COMMANDS_TABLE_PADDING,
    STYLE_COMMANDS_TABLE_ROW_STYLES,
    STYLE_COMMANDS_TABLE_SHOW_LINES,
    STYLE_FOOTER_TEXT,
    STYLE_HEADER_TEXT,
    STYLE_METAVAR,
    STYLE_OPTIONS_PANEL_BORDER,
    STYLE_OPTIONS_TABLE_BORDER_STYLE,
    STYLE_OPTIONS_TABLE_BOX,
    STYLE_OPTIONS_TABLE_LEADING,
    STYLE_OPTIONS_TABLE_PAD_EDGE,
    STYLE_OPTIONS_TABLE_PADDING,
    STYLE_OPTIONS_TABLE_ROW_STYLES,
    STYLE_OPTIONS_TABLE_SHOW_LINES,
    STYLE_REQUIRED_SHORT,
    USE_CLICK_SHORT_HELP,
    _get_parameter_help,
    _make_command_help,
    _make_rich_rext,
    highlighter,
)
from typing import List, Union

from kiara import ValueMap
from kiara.interfaces.python_api import KiaraAPI, OperationGroupInfo
from kiara.models.module.operation import Operation

# from kiara.interfaces.python_api.operation import KiaraOperation
from kiara.operations.included_core_operations.filter import FilterOperationType
from kiara.utils.cli import terminal_print
from kiara.utils.operations import create_operation_status_renderable

# MIT License
# Copyright (c) 2022 Phil Ewels
# adapted from: https://github.com/ewels/rich-click


def rich_format_filter_operation_help(
    api: KiaraAPI,
    obj: Union[click.Command, click.Group],
    ctx: click.Context,
    cmd_help: str,
    value: Union[None, str] = None,
) -> None:
    """Print nicely formatted help text using rich."""

    renderables: List[RenderableType] = []
    # Header text if we have it
    if HEADER_TEXT:
        renderables.append(
            Padding(_make_rich_rext(HEADER_TEXT, STYLE_HEADER_TEXT), (1, 1, 0, 1))
        )

    # Print usage

    _cmd = cmd_help
    renderables.append(Padding(_cmd, 1))
    d = inspect.getdoc(obj)
    if d is None:
        d = ""
    d = textwrap.dedent(d)
    renderables.append(
        Padding(
            Align(d, width=MAX_WIDTH, pad=False),  # type: ignore
            (0, 1, 1, 1),
        )
    )

    v = None
    if value:
        filter_op_type: FilterOperationType = api.get_operation_type("filter")  # type: ignore
        v = api.get_value(value)
        ops = filter_op_type.find_filter_operations_for_data_type(v.data_type_name)
        ops_info = OperationGroupInfo.create_from_operations(
            kiara=api.context, group_title=f"{v.data_type_name} filters", **ops
        )
        p = Panel(
            ops_info,
            title=f"Available filter operations for type [i]'{v.data_type_name}'[/i]",
            title_align="left",
        )
        renderables.append(p)

    # Epilogue if we have it
    if obj.epilog:
        # Remove single linebreaks, replace double with single
        lines = obj.epilog.split("\n\n")
        epilogue = "\n".join([x.replace("\n", " ").strip() for x in lines])
        renderables.append(
            Padding(Align(highlighter(epilogue), width=MAX_WIDTH, pad=False), 1)
        )

    # Footer text if we have it
    if FOOTER_TEXT:
        renderables.append(
            Padding(_make_rich_rext(FOOTER_TEXT, STYLE_FOOTER_TEXT), (1, 1, 0, 1))
        )

    terminal_print(Group(*renderables))


def rich_format_operation_help(
    obj: Union[click.Command, click.Group],
    ctx: click.Context,
    operation: Operation,
    op_inputs: ValueMap,
    cmd_help: str,
) -> None:
    """Print nicely formatted help text using rich.

    Based on original code from rich-cli, by @willmcgugan.
    https://github.com/Textualize/rich-cli/blob/8a2767c7a340715fc6fbf4930ace717b9b2fc5e5/src/rich_cli/__main__.py#L162-L236

    Replacement for the click function format_help().
    Takes a command or group and builds the help text output.

    Args:
        obj (click.Command or click.Group): Command or group to build help text for
        ctx (click.Context): Click Context object
        table: a rich table, including all the inputs of the current operation
    """

    renderables: List[RenderableType] = []
    # Header text if we have it
    if HEADER_TEXT:
        renderables.append(
            Padding(_make_rich_rext(HEADER_TEXT, STYLE_HEADER_TEXT), (1, 1, 0, 1))
        )

    # Print usage

    _cmd = cmd_help
    renderables.append(Padding(_cmd, 1))
    # renderables.append(obj.get_usage(ctx))
    # renderables.append(Panel(Padding(highlighter(obj.get_usage(ctx)), 1), style=STYLE_USAGE_COMMAND, box=box.MINIMAL))

    # Print command / group help if we have some
    desc = operation.doc.full_doc
    renderables.append(
        Padding(
            Align(Markdown(desc), width=MAX_WIDTH, pad=False),
            (0, 1, 1, 1),
        )
    )

    # if obj.help:
    #
    #     # Print with a max width and some padding
    #     renderables.append(
    #         Padding(
    #             Align(_get_help_text(obj), width=MAX_WIDTH, pad=False),
    #             (0, 1, 1, 1),
    #         )
    #     )

    # Look through OPTION_GROUPS for this command
    # stick anything unmatched into a default group at the end
    option_groups = OPTION_GROUPS.get(ctx.command_path, []).copy()
    option_groups.append({"options": []})
    argument_group_options = []

    for param in obj.get_params(ctx):

        # Skip positional arguments - they don't have opts or helptext and are covered in usage
        # See https://click.palletsprojects.com/en/8.0.x/documentation/#documenting-arguments
        if type(param) is click.core.Argument and not SHOW_ARGUMENTS:
            continue

        # Skip if option is hidden
        if getattr(param, "hidden", False):
            continue

        # Already mentioned in a config option group
        for option_group in option_groups:
            if any([opt in option_group.get("options", []) for opt in param.opts]):
                break

        # No break, no mention - add to the default group
        else:
            if type(param) is click.core.Argument and not GROUP_ARGUMENTS_OPTIONS:
                argument_group_options.append(param.opts[0])
            else:
                list_of_option_groups: List = option_groups[-1]["options"]  # type: ignore
                list_of_option_groups.append(param.opts[0])

    # If we're not grouping arguments and we got some, prepend before default options
    if len(argument_group_options) > 0:
        extra_option_group = {
            "name": ARGUMENTS_PANEL_TITLE,
            "options": argument_group_options,
        }
        option_groups.insert(len(option_groups) - 1, extra_option_group)  # type: ignore

    # Print each option group panel
    for option_group in option_groups:

        options_rows = []
        for opt in option_group.get("options", []):

            # Get the param
            for param in obj.get_params(ctx):
                if any([opt in param.opts]):
                    break
            # Skip if option is not listed in this group
            else:
                continue

            # Short and long form
            opt_long_strs = []
            opt_short_strs = []
            for idx, opt in enumerate(param.opts):
                opt_str = opt
                try:
                    opt_str += "/" + param.secondary_opts[idx]
                except IndexError:
                    pass
                if "--" in opt:
                    opt_long_strs.append(opt_str)
                else:
                    opt_short_strs.append(opt_str)

            # Column for a metavar, if we have one
            metavar = Text(style=STYLE_METAVAR, overflow="fold")
            metavar_str = param.make_metavar()

            # Do it ourselves if this is a positional argument
            if type(param) is click.core.Argument and metavar_str == param.name.upper():  # type: ignore
                metavar_str = param.type.name.upper()

            # Skip booleans and choices (handled above)
            if metavar_str != "BOOLEAN":
                metavar.append(metavar_str)

            # Range - from
            # https://github.com/pallets/click/blob/c63c70dabd3f86ca68678b4f00951f78f52d0270/src/click/core.py#L2698-L2706  # noqa: E501
            try:
                # skip count with default range type
                if isinstance(param.type, click.types._NumberRangeBase) and not (
                    param.count and param.type.min == 0 and param.type.max is None  # type: ignore
                ):
                    range_str = param.type._describe_range()
                    if range_str:
                        metavar.append(RANGE_STRING.format(range_str))
            except AttributeError:
                # click.types._NumberRangeBase is only in Click 8x onwards
                pass

            # Required asterisk
            required: RenderableType = ""
            if param.required:
                required = Text(REQUIRED_SHORT_STRING, style=STYLE_REQUIRED_SHORT)

            # Highlighter to make [ | ] and <> dim
            class MetavarHighlighter(RegexHighlighter):
                highlights = [
                    r"^(?P<metavar_sep>(\[|<))",
                    r"(?P<metavar_sep>\|)",
                    r"(?P<metavar_sep>(\]|>)$)",
                ]

            metavar_highlighter = MetavarHighlighter()

            rows = [
                required,
                highlighter(highlighter(",".join(opt_long_strs))),
                highlighter(highlighter(",".join(opt_short_strs))),
                metavar_highlighter(metavar),
                _get_parameter_help(param, ctx),  # type: ignore
            ]

            # Remove metavar if specified in config
            if not SHOW_METAVARS_COLUMN:
                rows.pop(3)

            options_rows.append(rows)

        if len(options_rows) > 0:
            t_styles = {
                "show_lines": STYLE_OPTIONS_TABLE_SHOW_LINES,
                "leading": STYLE_OPTIONS_TABLE_LEADING,
                "box": STYLE_OPTIONS_TABLE_BOX,
                "border_style": STYLE_OPTIONS_TABLE_BORDER_STYLE,
                "row_styles": STYLE_OPTIONS_TABLE_ROW_STYLES,
                "pad_edge": STYLE_OPTIONS_TABLE_PAD_EDGE,
                "padding": STYLE_OPTIONS_TABLE_PADDING,
            }
            t_styles.update(option_group.get("table_styles", {}))  # type: ignore
            box_style = getattr(box, t_styles.pop("box"), None)  # type: ignore

            options_table = Table(
                highlight=True,
                show_header=False,
                expand=True,
                box=box_style,
                **t_styles,  # type: ignore
            )
            # Strip the required column if none are required
            if all([x[0] == "" for x in options_rows]):
                options_rows = [x[1:] for x in options_rows]
            for row in options_rows:
                options_table.add_row(*row)
            renderables.append(
                Panel(
                    options_table,
                    border_style=STYLE_OPTIONS_PANEL_BORDER,  # type: ignore
                    title=option_group.get("name", OPTIONS_PANEL_TITLE),  # type: ignore
                    title_align=ALIGN_OPTIONS_PANEL,  # type: ignore
                    width=MAX_WIDTH,  # type: ignore
                )
            )

    #
    # Groups only:
    # List click command groups
    #
    if hasattr(obj, "list_commands"):
        # Look through COMMAND_GROUPS for this command
        # stick anything unmatched into a default group at the end
        cmd_groups = COMMAND_GROUPS.get(ctx.command_path, []).copy()
        cmd_groups.append({"commands": []})
        for command in obj.list_commands(ctx):  # type: ignore
            for cmd_group in cmd_groups:
                if command in cmd_group.get("commands", []):
                    break
            else:
                commands: List = cmd_groups[-1]["commands"]  # type: ignore
                commands.append(command)

        # Print each command group panel
        for cmd_group in cmd_groups:
            t_styles = {
                "show_lines": STYLE_COMMANDS_TABLE_SHOW_LINES,
                "leading": STYLE_COMMANDS_TABLE_LEADING,
                "box": STYLE_COMMANDS_TABLE_BOX,
                "border_style": STYLE_COMMANDS_TABLE_BORDER_STYLE,
                "row_styles": STYLE_COMMANDS_TABLE_ROW_STYLES,
                "pad_edge": STYLE_COMMANDS_TABLE_PAD_EDGE,
                "padding": STYLE_COMMANDS_TABLE_PADDING,
            }
            t_styles.update(cmd_group.get("table_styles", {}))  # type: ignore
            box_style = getattr(box, t_styles.pop("box"), None)  # type: ignore

            commands_table = Table(
                highlight=False,
                show_header=False,
                expand=True,
                box=box_style,  # type: ignore
                **t_styles,  # type: ignore
            )
            # Define formatting in first column, as commands don't match highlighter regex
            commands_table.add_column(style="bold cyan", no_wrap=True)
            for command in cmd_group.get("commands", []):
                # Skip if command does not exist
                if command not in obj.list_commands(ctx):  # type: ignore
                    continue
                cmd = obj.get_command(ctx, command)  # type: ignore
                assert cmd is not None
                if cmd.hidden:
                    continue
                # Use the truncated short text as with vanilla text if requested
                if USE_CLICK_SHORT_HELP:
                    helptext = cmd.get_short_help_str()
                else:
                    # Use short_help function argument if used, or the full help
                    helptext = cmd.short_help or cmd.help or ""
                commands_table.add_row(command, _make_command_help(helptext))
            if commands_table.row_count > 0:
                renderables.append(
                    Panel(
                        commands_table,
                        border_style=STYLE_COMMANDS_PANEL_BORDER,  # type: ignore
                        title=cmd_group.get("name", COMMANDS_PANEL_TITLE),  # type: ignore
                        title_align=ALIGN_COMMANDS_PANEL,  # type: ignore
                        width=MAX_WIDTH,  # type: ignore
                    )
                )

    inputs_table = create_operation_status_renderable(
        operation=operation,
        inputs=op_inputs,
        render_config={
            "show_operation_name": False,
            "show_inputs": True,
            "show_outputs_schema": False,
            "show_headers": False,
            "show_operation_doc": False,
        },
    )
    # inputs_table = operation.create_renderable(
    #     show_operation_name=False,
    #     show_operation_doc=False,
    #     show_inputs=True,
    #     show_outputs_schema=False,
    #     show_headers=False,
    # )

    inputs_panel = Panel(
        inputs_table,
        title="Inputs",
        border_style=STYLE_COMMANDS_PANEL_BORDER,  # type: ignore
        title_align=ALIGN_COMMANDS_PANEL,  # type: ignore
        width=MAX_WIDTH,  # type: ignore
    )
    renderables.append(inputs_panel)

    # Epilogue if we have it
    if obj.epilog:
        # Remove single linebreaks, replace double with single
        lines = obj.epilog.split("\n\n")
        epilogue = "\n".join([x.replace("\n", " ").strip() for x in lines])
        renderables.append(
            Padding(Align(highlighter(epilogue), width=MAX_WIDTH, pad=False), 1)
        )

    # Footer text if we have it
    if FOOTER_TEXT:
        renderables.append(
            Padding(_make_rich_rext(FOOTER_TEXT, STYLE_FOOTER_TEXT), (1, 1, 0, 1))
        )

    group = Group(*renderables)
    terminal_print(group)
