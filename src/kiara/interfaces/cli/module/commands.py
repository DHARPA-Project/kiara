# -*- coding: utf-8 -*-
"""Module related subcommands for the cli."""
import asyncclick as click
import os.path
import sys
import typing
from jupytext import jupytext
from rich.panel import Panel

from kiara import Kiara, PipelineModule
from kiara.defaults import KIARA_RESOURCES_FOLDER
from kiara.interfaces.cli.utils import _create_module_instance
from kiara.module import ModuleInfo
from kiara.rendering.pipeline import PipelineRenderer
from kiara.utils import dict_from_cli_args
from kiara.utils.output import rich_print


@click.group()
@click.pass_context
def module(ctx):
    """Information about available modules, and details about them."""


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
@click.pass_context
def list_modules(ctx, only_pipeline_modules: bool, only_core_modules: bool):
    """List available module types."""

    if only_pipeline_modules and only_core_modules:
        rich_print()
        rich_print(
            "Please provide either '--only-core-modules' or '--only-pipeline-modules', not both."
        )
        sys.exit(1)

    kiara_obj: Kiara = ctx.obj["kiara"]

    if only_pipeline_modules:
        title = "Available pipeline modules"
        m_list = kiara_obj.create_modules_list(
            list_pipeline_modules=True, list_non_pipeline_modules=False
        )
    elif only_core_modules:
        title = "Available core modules"
        m_list = kiara_obj.create_modules_list(
            list_pipeline_modules=False, list_non_pipeline_modules=True
        )
    else:
        title = "Available modules"
        m_list = kiara_obj.modules_list

    p = Panel(m_list, title_align="left", title=title)
    print()
    kiara_obj.explain(p)


@module.command(name="explain-type")
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
    info = ModuleInfo.from_module_cls(m_cls)

    rich_print()
    rich_print(info)


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

    module_obj = _create_module_instance(
        ctx, module_type=module_type, module_config=module_config
    )
    rich_print()
    rich_print(module_obj)


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
    path = os.path.join(KIARA_RESOURCES_FOLDER, "templates", "notebook.ipynb.j2")
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
