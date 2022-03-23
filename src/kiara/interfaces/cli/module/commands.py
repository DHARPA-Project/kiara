# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Module related subcommands for the cli."""

import os.path
import rich_click as click
import sys
import typing
from rich.panel import Panel

from kiara import Kiara

# from kiara.interfaces.cli.utils import _create_module_instance
from kiara.kiara import explain
from kiara.models.module import KiaraModuleTypeMetadata, ModuleTypesGroupInfo
from kiara.models.module.manifest import Manifest
from kiara.utils import dict_from_cli_args, log_message, rich_print


@click.group()
@click.pass_context
def module(ctx):
    """Module-related sub-commands.."""


@module.command(name="list")
@click.option(
    "--only-pipeline-modules", "-p", is_flag=True, help="Only list pipeline modules."
)
@click.option(
    "--only-core-modules",
    "-c",
    is_flag=True,
    help="Only list core (aka 'Python') modules.",
)
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full documentation for every module type.",
)
@click.argument("filter", nargs=-1, required=False)
@click.pass_context
def list_modules(
    ctx,
    only_pipeline_modules: bool,
    only_core_modules: bool,
    full_doc: bool,
    filter: typing.Iterable[str],
):
    """List available module data_types."""

    if only_pipeline_modules and only_core_modules:
        rich_print()
        rich_print(
            "Please provide either '--only-core-modules' or '--only-pipeline-modules', not both."
        )
        sys.exit(1)

    kiara_obj: Kiara = ctx.obj["kiara"]

    if filter:
        module_types = []

        for m in kiara_obj.module_type_names:
            match = True

            for f in filter:

                if f.lower() not in m.lower():
                    match = False
                    break
                # else:
                #     m_cls = kiara_obj.get_module_class(m)
                #     doc = m_cls.get_type_metadata().documentation.full_doc
                #
                #     if f.lower() not in doc.lower():
                #         match = False
                #         break

            if match:
                module_types.append(m)
    else:
        module_types = kiara_obj.module_type_names

    renderable = ModuleTypesGroupInfo.create_renderable_from_type_names(
        kiara=kiara_obj,
        type_names=module_types,
        ignore_non_pipeline_modules=only_pipeline_modules,
        ignore_pipeline_modules=only_core_modules,
        include_full_doc=full_doc,
    )
    if only_pipeline_modules:
        title = "Available pipeline modules"
    elif only_core_modules:
        title = "Available core modules"
    else:
        title = "Available modules"

    p = Panel(renderable, title_align="left", title=title)
    print()
    explain(p)


@module.command(name="explain")
@click.argument("module_type", nargs=1, required=True)
@click.pass_context
def explain_module_type(ctx, module_type: str):
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

    m_cls = kiara_obj.get_module_class(_module_type)
    info = KiaraModuleTypeMetadata.from_module_class(m_cls)

    rich_print()
    rich_print(info.create_panel(title=f"Module type: [b i]{module_type}[/b i]"))


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
