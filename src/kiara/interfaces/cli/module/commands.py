# -*- coding: utf-8 -*-
"""Module related subcommands for the cli."""
import click
import os.path
import sys
import typing
from rich.panel import Panel

from kiara import Kiara, PipelineModule
from kiara.defaults import KIARA_RESOURCES_FOLDER
from kiara.info.modules import ModuleTypesGroupInfo
from kiara.interfaces.cli.utils import _create_module_instance
from kiara.metadata.module_models import KiaraModuleTypeMetadata
from kiara.utils import dict_from_cli_args, is_develop, log_message
from kiara.utils.modules import find_all_module_python_files, find_file_for_module
from kiara.utils.output import rich_print


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
    """List available module types."""

    if only_pipeline_modules and only_core_modules:
        rich_print()
        rich_print(
            "Please provide either '--only-core-modules' or '--only-pipeline-modules', not both."
        )
        sys.exit(1)

    kiara_obj: Kiara = ctx.obj["kiara"]

    if filter:
        module_types = []

        for m in kiara_obj.available_module_types:
            match = True

            for f in filter:

                if f.lower() not in m.lower():
                    match = False
                    break
                else:
                    m_cls = kiara_obj.get_module_class(m)
                    doc = m_cls.get_type_metadata().documentation.full_doc

                    if f.lower() not in doc.lower():
                        match = False
                        break

            if match:
                module_types.append(m)
    else:
        module_types = kiara_obj.available_module_types

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
    kiara_obj.explain(p)


@module.command(name="explain")
@click.argument("module_type", nargs=1, required=True)
@click.pass_context
def explain_module_type(ctx, module_type: str):
    """Print details of a module type.

    This is different to the 'explain-instance' command, because module types need to be
    instantiated with configuration, before we can query all their properties (like
    input/output types).
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

    module_obj = _create_module_instance(
        ctx, module_type=module_type, module_config=module_config
    )
    rich_print()
    rich_print(module_obj)


try:

    from kiara_streamlit.defaults import MODULE_DEV_STREAMLIT_FILE  # type: ignore
    from streamlit import bootstrap
    from streamlit.cli import ACCEPTED_FILE_EXTENSIONS, _main_run, configurator_options

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

        if module_name:

            if module_name not in kiara_obj.available_module_types:
                print()
                print(
                    f"Can't launch dev UI for module '{module_name}': module not available."
                )
                sys.exit(1)

            python_path_to_watch = find_file_for_module(
                module_name=module_name, kiara=kiara_obj
            )

            _python_path = os.environ.get("PYTHONPATH", None)
            if _python_path is None:
                python_path = []
            else:
                python_path = _python_path.split(":")

            if python_path_to_watch not in python_path:
                python_path.append(python_path_to_watch)

            os.environ["DEV_MODULE_NAME"] = module_name

        else:
            all_paths = find_all_module_python_files(kiara=kiara_obj)
            _python_path = os.environ.get("PYTHONPATH", None)
            if _python_path is None:
                python_path = []
            else:
                python_path = _python_path.split(":")

            filtered = (
                p for p in all_paths if f"{os.sep}kiara{os.sep}src{os.sep}" not in p
            )
            python_path.extend(filtered)

        python_path_export = ":".join(python_path)
        os.environ["PYTHONPATH"] = python_path_export

        bootstrap.load_config_options(flag_options=kwargs)
        target = MODULE_DEV_STREAMLIT_FILE

        _, extension = os.path.splitext(target)
        if extension[1:] not in ACCEPTED_FILE_EXTENSIONS:
            if extension[1:] == "":
                raise click.BadArgumentUsage(
                    "Streamlit requires raw Python (.py) files, but the provided file has no extension.\nFor more information, please see https://docs.streamlit.io"
                )
            else:
                raise click.BadArgumentUsage(
                    "Streamlit requires raw Python (.py) files, not %s.\nFor more information, please see https://docs.streamlit.io"
                    % extension
                )

        if not os.path.exists(target):
            raise click.BadParameter("File does not exist: {}".format(target))
        _main_run(target, args, flag_options=kwargs)


except Exception:
    log_message(
        "'kiara.streamlit' package not installed, not offering streamlit debug sub-command"
    )

if is_develop():
    try:
        from jupytext import jupytext

        from kiara.rendering.pipeline import PipelineRenderer

        @module.command("render")
        @click.argument("module_type", nargs=1)
        @click.argument("inputs", nargs=-1, required=False)
        @click.option(
            "--module-config",
            "-c",
            required=False,
            help="(Optional) module configuration.",
            multiple=True,
        )
        @click.pass_context
        def render(
            ctx,
            module_type: str,
            module_config: typing.Iterable[typing.Any],
            inputs: typing.Any,
        ):
            """Render a workflow into a jupyter notebook."""

            if module_config:
                module_config = dict_from_cli_args(*module_config)

            module_obj: PipelineModule = _create_module_instance(  # type: ignore
                ctx, module_type=module_type, module_config=module_config
            )
            if not module_obj.is_pipeline():
                print("Only pipeline modules supported (for now).")
                sys.exit(1)

            structure = module_obj.structure

            list_keys = []
            for name, value_schema in module_obj.input_schemas.items():
                if value_schema.type in ["array", "list"]:
                    list_keys.append(name)
            workflow_input = dict_from_cli_args(*inputs, list_keys=list_keys)

            renderer = PipelineRenderer(structure=structure)
            path = os.path.join(
                KIARA_RESOURCES_FOLDER, "templates", "notebook.ipynb.j2"
            )
            # path = os.path.join(KIARA_RESOURCES_FOLDER, "templates", "python_script.py.j2")

            step_inputs: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
            for k, v in workflow_input.items():
                pi = structure.pipeline_inputs.get(k)
                assert pi
                if len(pi.connected_inputs) != 1:
                    raise NotImplementedError()

                ci = pi.connected_inputs[0]
                if isinstance(v, str):
                    v = f'"{v}"'
                step_inputs.setdefault(ci.step_id, {})[ci.value_name] = v

            rendered = renderer.render_from_path(path, inputs=step_inputs)
            print()
            # print(rendered)
            # return
            # print(rendered)
            notebook = jupytext.reads(rendered, fmt="py:percent")
            converted = jupytext.writes(notebook, fmt="notebook")
            print(converted)

    except ModuleNotFoundError:
        pass
