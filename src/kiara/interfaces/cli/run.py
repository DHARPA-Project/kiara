# -*- coding: utf-8 -*-
"""The 'run' subcommand for the cli."""

import asyncclick as click
import os.path
import sys
import typing
from kiara_modules.core.json import DEFAULT_TO_JSON_CONFIG
from pathlib import Path
from rich import box
from rich.console import Console, RenderGroup
from rich.panel import Panel
from rich.syntax import Syntax

from kiara import Kiara
from kiara.data.values import ValuesInfo
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.interfaces.cli.utils import _create_module_instance
from kiara.module import KiaraModule
from kiara.pipeline.controller import BatchController
from kiara.pipeline.pipeline import StepStatus
from kiara.processing.parallel import ThreadPoolProcessor
from kiara.utils import create_table_from_field_schemas, dict_from_cli_args, is_debug
from kiara.utils.output import OutputDetails, rich_print


@click.command()
@click.argument("module", nargs=1)
@click.argument("inputs", nargs=-1, required=False)
@click.option("--id", "-i", help="Set workflow id.", required=False)
@click.option(
    "--module-config",
    "-c",
    required=False,
    help="(Optional) module configuration.",
    multiple=True,
)
@click.option(
    "--explain",
    "-e",
    help="Display additional workflow details.",
    is_flag=True,
)
@click.option(
    "--output", "-o", help="The output format and configuration.", multiple=True
)
@click.option(
    "--save", "-s", help="Save the outputs into the kiara data store.", is_flag=True
)
@click.pass_context
async def run(ctx, module, inputs, module_config, output, explain, save, id):
    """Execute a workflow run."""

    if module_config:
        module_config = dict_from_cli_args(*module_config)

    kiara_obj: Kiara = ctx.obj["kiara"]

    if module in kiara_obj.available_module_types:
        module_name = module
    elif f"core.{module}" in kiara_obj.available_module_types:
        module_name = f"core.{module}"
    elif os.path.isfile(module):
        module_name = kiara_obj.register_pipeline_description(
            module, raise_exception=True
        )
    else:
        rich_print(
            f"\nInvalid module name '[i]{module}[/i]'. Must be a path to a pipeline file, or one of the available modules:\n"
        )
        for n in kiara_obj.available_module_types:
            rich_print(f"  - [i]{n}[/i]")
        sys.exit(1)

    if not inputs:

        module_obj: KiaraModule = _create_module_instance(
            ctx=ctx, module_type=module_name, module_config=module_config
        )

        one_required = False
        for input_name in module_obj.input_names:
            if module_obj.input_required(input_name):
                one_required = True
                break

        if one_required:

            inputs_table = create_table_from_field_schemas(
                _show_header=True, **module_obj.input_schemas
            )
            print()
            print(
                "No inputs provided, not running the workflow. To run it, provide input following this schema:"
            )
            rich_print(inputs_table)
            sys.exit(0)

    output_details = OutputDetails.from_data(output)
    silent = False
    if output_details.format == "silent":
        silent = True

    force_overwrite = output_details.config.get("force", False)

    # SUPPORTED_TARGETS = ["terminal", "file"]
    # if output_details.target not in SUPPORTED_TARGETS:
    #     print()
    #     rich_print(f"Invalid output target '{output_details.target}', must be one of: [i]{', '.join(SUPPORTED_TARGETS)}[/i]")
    #     sys.exit(1)

    target_file: typing.Optional[Path] = None
    if output_details.target != "terminal":
        if output_details.target == "file":
            target_dir = Path.cwd()
            target_file = target_dir / f"{module_name}.{output_details.format}"
        else:
            target_file = Path(
                os.path.realpath(os.path.expanduser(output_details.target))
            )

        if target_file.exists() and not force_overwrite:
            print()
            print(
                f"Can't run workflow, the target files already exist, and '--output force=true' not specified: {target_file}"
            )
            sys.exit(1)

    processor = ThreadPoolProcessor()
    processor = None
    controller = BatchController(processor=processor)

    workflow_id = id
    if workflow_id is None:
        workflow_id = f"{module_name}_0"

    workflow = kiara_obj.create_workflow(
        module_name,
        module_config=module_config,
        workflow_id=workflow_id,
        controller=controller,
    )

    if save:

        invalid = set()
        for ov in workflow.outputs.values():
            existing = kiara_obj.data_store.check_existing_aliases(*ov.aliases)
            invalid.update(existing)

        if invalid:
            print()
            print(
                f"Can't run workflow, value aliases for saving already exist: {', '.join(invalid)}. Set another workflow id?"
            )
            sys.exit(1)

    list_keys = []

    for name, value in workflow.inputs.items():
        if value.value_schema.type in ["array", "list"]:
            list_keys.append(name)

    workflow_input = dict_from_cli_args(*inputs, list_keys=list_keys)
    failed = False
    try:
        if workflow_input:
            workflow.inputs.set_values(**workflow_input)
        else:
            workflow.controller.process_pipeline()
    except Exception as e:
        print()
        print(e)
        failed = True

    if explain:
        print()
        kiara_obj.explain(workflow.current_state)

        if workflow.status == StepStatus.RESULTS_READY:
            vi = ValuesInfo(workflow.outputs)
            vi_table = vi.create_value_info_table(
                ensure_metadata=True, show_headers=True
            )
            panel = Panel(Panel(vi_table), box=box.SIMPLE)
            rich_print("[b]Output data details[/b]")
            rich_print(panel)

    if failed:
        sys.exit(1)

    if not silent:

        if output_details.target == "terminal":
            if output_details.format == "terminal":
                print()
                pretty_print = kiara_obj.create_workflow("string.pretty_print")
                pretty_print_inputs: typing.Dict[str, typing.Any] = {
                    "item": workflow.outputs
                }
                pretty_print_inputs.update(DEFAULT_PRETTY_PRINT_CONFIG)

                pretty_print.inputs.set_values(**pretty_print_inputs)

                renderables = pretty_print.outputs.get_value_data("renderables")
                if renderables:
                    output = Panel(RenderGroup(*renderables), box=box.SIMPLE)
                    rich_print("[b]Output data[/b]")
                    rich_print(output)
                else:
                    rich_print("No output.")
            else:

                format = output_details.format
                available_formats = kiara_obj.get_convert_target_types(
                    source_type="value_set"
                )
                if format not in available_formats:
                    print()
                    print(
                        f"Can't convert to output format '{format}', this format is not supported. Available formats: {', '.join(available_formats)}."
                    )
                    sys.exit(1)

                config = {}
                config.update(DEFAULT_TO_JSON_CONFIG)

                try:
                    transformed = kiara_obj.transform_data(
                        workflow.outputs,
                        source_type="value_set",
                        target_type=format,
                        config=config,
                    )
                    transformed_value = transformed.get_value_data("target_value")

                    if format in ["json", "yaml"]:
                        transformed_str = Syntax(
                            transformed_value,
                            lexer_name=format,
                            background_color="default",
                        )
                        rich_print(transformed_str)
                    else:
                        print(transformed_value)
                except Exception as e:
                    print()
                    rich_print(f"Can't transform outputs into '{format}': {e}")
                    sys.exit(1)

        else:
            if output_details.format == "terminal":

                pretty_print = kiara_obj.create_workflow("string.pretty_print")

                pretty_print_inputs = {"item": value}
                pretty_print_inputs.update(DEFAULT_PRETTY_PRINT_CONFIG)
                pretty_print.inputs.set_values(**pretty_print_inputs)

                renderables = pretty_print.outputs.get_value_data("renderables")
                output = Panel(RenderGroup(*renderables), box=box.SIMPLE)
                with open(target_file, "wt") as f:
                    console = Console(record=True, file=f)
                    console.print(output)
            else:

                format = output_details.format
                available_formats = kiara_obj.get_convert_target_types(
                    source_type="value_set"
                )
                if format not in available_formats:
                    print()
                    print(
                        f"Can't convert to output format '{format}', this format is not supported. Available formats: {', '.join(available_formats)}."
                    )
                    sys.exit(1)

                config = {}
                config.update(DEFAULT_TO_JSON_CONFIG)

                transformed = kiara_obj.transform_data(
                    workflow.outputs,
                    source_type="value_set",
                    target_type=format,
                    config=config,
                )
                transformed_value = transformed.get_value_data()

                target_file.parent.mkdir(parents=True, exist_ok=True)
                # TODO: check whether to write text or bytes
                target_file.write_text(transformed_value)

    if save:
        for field, value in workflow.outputs.items():
            rich_print(f"Saving '[i]{field}[/i]'...")
            try:
                value_id = value.save()
                rich_print(f"   -> done, id: [i]{value_id}[/i]")

            except Exception as e:
                if is_debug():
                    import traceback

                    traceback.print_exc()
                rich_print(f"   -> failed: [red]{e}[/red]")
            print()
