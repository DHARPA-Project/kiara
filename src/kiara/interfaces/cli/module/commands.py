# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Module related subcommands for the cli."""

import os.path
import rich_click as click
from typing import Any, Iterable, List

from kiara import Kiara

# from kiara.interfaces.cli.utils import _create_module_instance
from kiara.models.module import KiaraModuleTypeInfo, ModuleTypeClassesInfo
from kiara.models.module.manifest import Manifest
from kiara.utils import dict_from_cli_args, log_message
from kiara.utils.cli import output_format_option, terminal_print_model


@click.group()
@click.pass_context
def module(ctx):
    """Module-related sub-commands."""


@module.command(name="list")
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full documentation for every module type (when using 'terminal' output format).",
)
@output_format_option()
@click.argument("filter", nargs=-1, required=False)
@click.pass_context
def list_modules(ctx, full_doc: bool, filter: Iterable[str], format: str):
    """List available module data_types."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    if filter:
        title = f"Filtered modules: {filter}"
        module_types_names: List[str] = []

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
        module_types_names = list(kiara_obj.module_type_names)

    module_types = {
        n: kiara_obj.module_registry.get_module_class(n) for n in module_types_names
    }

    module_types_info = ModuleTypeClassesInfo.create_from_type_items(
        group_alias=title, **module_types
    )

    terminal_print_model(module_types_info, format=format, in_panel=title)


@module.command(name="explain")
@click.argument("module_type", nargs=1, required=True)
@output_format_option()
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

    terminal_print_model(
        info, format=format, in_panel=f"Module type: [b i]{module_type}[/b i]"
    )


@module.command("explain-instance")
@click.argument("module_type", nargs=1)
@click.argument(
    "module_config",
    nargs=-1,
)
@output_format_option()
@click.pass_context
def explain_module(ctx, module_type: str, module_config: Iterable[Any], format: str):
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

    terminal_print_model(
        module_obj.create_renderable(),  # type: ignore
        format=format,
        in_panel=f"Module instance of type: [b i]{module_type}[/b i]",
    )


try:

    from kiara_streamlit.defaults import (  # type: ignore
        MODULE_DEV_STREAMLIT_FILE,
        MODULE_INFO_UI_STREAMLIT_FILE,
    )
    from kiara_streamlit.utils import run_streamlit  # type: ignore
    from streamlit.cli import configurator_options  # type: ignore

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
