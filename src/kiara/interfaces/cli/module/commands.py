# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Module related subcommands for the cli."""

import orjson
import os.path
import rich_click as click
import typing
from rich.panel import Panel

from kiara import Kiara

# from kiara.interfaces.cli.utils import _create_module_instance
from kiara.kiara import explain
from kiara.models.module import KiaraModuleTypeInfo, ModuleTypeClassesInfo
from kiara.models.module.manifest import Manifest
from kiara.utils import dict_from_cli_args, log_message, rich_print


@click.group()
@click.pass_context
def module(ctx):
    """Module-related sub-commands.."""


@module.command(name="list")
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full documentation for every module type (when using 'terminal' output format).",
)
@click.option(
    "--format",
    "-f",
    help="The output format. Defaults to 'terminal'.",
    type=click.Choice(["terminal", "json", "html"]),
    default="terminal",
)
@click.argument("filter", nargs=-1, required=False)
@click.pass_context
def list_modules(ctx, full_doc: bool, filter: typing.Iterable[str], format: str):
    """List available module data_types."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    if filter:
        title = f"Filtered modules: {filter}"
        module_types_names = []

        for m in kiara_obj.module_type_names:
            match = True

            for f in filter:

                if f.lower() not in m.lower():
                    match = False
                    break

            if match:
                module_types_names.append(m)
    else:
        title = "All modules"
        module_types_names = kiara_obj.module_type_names

    module_types = {
        n: kiara_obj.module_registry.get_module_class(n) for n in module_types_names
    }

    module_types_info = ModuleTypeClassesInfo.create_from_type_items(
        group_alias=title, **module_types
    )

    if format == "terminal":
        renderable = module_types_info.create_renderable(full_doc=full_doc)
        p = Panel(renderable, title_align="left", title=title)
        print()
        explain(p)
    elif format == "json":
        print(module_types_info.json(option=orjson.OPT_INDENT_2))
    elif format == "html":
        print(module_types_info.create_html())


@module.command(name="explain")
@click.option(
    "--format",
    "-f",
    help="The output format. Defaults to 'terminal'.",
    type=click.Choice(["terminal", "json", "html"]),
    default="terminal",
)
@click.argument("module_type", nargs=1, required=True)
@click.pass_context
def explain_module_type(ctx, module_type: str, format: str):
    """Print details of a module type.

    This is different to the 'explain-instance' command, because module data_types need to be
    instantiated with configuration, before we can query all their properties (like
    input/output data_types).
    """

    kiara_obj: Kiara = ctx.obj["kiara"]

    if os.path.isfile(module_type):
        _module_type: str = kiara_obj.register_pipeline_description(  # type: ignore
            module_type, raise_exception=True
        )  # type: ignore
    else:
        _module_type = module_type

    m_cls = kiara_obj.module_registry.get_module_class(_module_type)
    info = KiaraModuleTypeInfo.create_from_type_class(m_cls)

    if format == "terminal":
        rich_print()
        rich_print(info.create_panel(title=f"Module type: [b i]{module_type}[/b i]"))
    elif format == "json":
        print(info.json(option=orjson.OPT_INDENT_2))
    elif format == "html":
        print(info.create_html())


@module.command("explain-instance")
@click.argument("module_type", nargs=1)
@click.argument(
    "module_config",
    nargs=-1,
)
@click.pass_context
def explain_module(ctx, module_type: str, module_config: typing.Iterable[typing.Any]):
    """Describe a module instance.

    This command shows information and metadata about an instantiated *kiara* module.
    """

    if module_config:
        module_config = dict_from_cli_args(*module_config)
    else:
        module_config = {}

    kiara_obj: Kiara = ctx.obj["kiara"]

    mc = Manifest(module_type=module_type, module_config=module_config)
    module_obj = kiara_obj.create_module(mc)

    rich_print()
    rich_print(module_obj)


try:

    from kiara_streamlit.defaults import (  # type: ignore
        MODULE_DEV_STREAMLIT_FILE,
        MODULE_INFO_UI_STREAMLIT_FILE,
    )
    from kiara_streamlit.utils import run_streamlit
    from streamlit.cli import configurator_options

    @module.command("dev")
    @configurator_options
    @click.argument("module_name", required=False, nargs=1)
    @click.argument("args", nargs=-1)
    @click.pass_context
    def dev_ui(ctx, module_name, args=None, **kwargs):
        """Auto-render web-ui to help with module development.

        If no module name is provided, a selection box will displayed in the published app.

        This subcommand uses [streamlit](https://streamlit.io) to auto-render a UI for a (single) module, incl. input fields,
        input previews, output previews, and debug messages. Its main purpose is to aid module development, but it can be
        used as a module execution UI in a pinch.
        """

        kiara_obj: Kiara = ctx.obj["kiara"]

        run_streamlit(
            kiara=kiara_obj,
            streamlit_app_path=MODULE_DEV_STREAMLIT_FILE,
            module_name=module_name,
            streamlit_flags=kwargs,
        )

    @module.command("info-ui")
    @configurator_options
    @click.argument("module_name", required=False, nargs=1)
    # @click.argument("args", nargs=-1)
    @click.pass_context
    def info_ui(ctx, module_name, **kwargs):
        """Auto-render web-ui to help with module development.

        If no module name is provided, a selection box will displayed in the published app.

        This subcommand uses [streamlit](https://streamlit.io) to auto-render a UI for a (single) module, incl. input fields,
        input previews, output previews, and debug messages. Its main purpose is to aid module development, but it can be
        used as a module execution UI in a pinch.
        """

        kiara_obj: Kiara = ctx.obj["kiara"]

        run_streamlit(
            kiara=kiara_obj,
            streamlit_app_path=MODULE_INFO_UI_STREAMLIT_FILE,
            module_name=module_name,
            streamlit_flags=kwargs,
        )


except Exception as e:  # noqa
    log_message(
        "'kiara.streamlit' package not installed, not offering streamlit debug sub-command"
    )
