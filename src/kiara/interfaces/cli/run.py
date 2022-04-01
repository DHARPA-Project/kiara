# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""The 'run' subcommand for the cli."""

import os.path
import rich_click as click
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from kiara import Kiara
from kiara.models.module.jobs import JobStatus, JobRecord, JobConfig
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import Operation
from kiara.models.values.value import ValueSet
from kiara.utils import dict_from_cli_args, is_debug, rich_print
from kiara.utils.output import OutputDetails
from kiara.utils.values import augment_values


@click.command()
@click.argument("module_or_operation", nargs=1, metavar="MODULE_OR_OPERATION")
@click.argument("inputs", nargs=-1, required=False)
@click.option(
    "--module-config",
    "-c",
    required=False,
    help="(Optional) module configuration, only valid when run target is a module name.",
    multiple=True,
)
@click.option(
    "--explain",
    "-e",
    help="Display additional runtime information.",
    is_flag=True,
)
@click.option(
    "--output", "-o", help="The output format and configuration.", multiple=True
)
@click.option(
    "--save",
    "-s",
    help="Save one or several of the outputs of this run. If the argument contains a '=', the format is [output_name]=[alias], if not, the values will be saved as '[alias]-[output_name]'.",
    required=False,
    multiple=True,
)
@click.pass_context
def run(
    ctx,
    module_or_operation: str,
    module_config: Iterable[str],
    inputs: Iterable[str],
    output: Iterable[str],
    explain: bool,
    save: Iterable[str],
):

    # =========================================================================
    # initialize a few variables
    if module_config:
        module_config = dict_from_cli_args(*module_config)
    else:
        module_config = {}

    if not save:
        aliases: Dict[str, List[str]] = {}
        full_aliases: List[str] = []
    else:
        aliases = {}
        full_aliases = []
        for a in save:
            if "=" not in a:
                full_aliases.append(a)
            else:
                tokens = a.split("=")
                if len(tokens) != 2:
                    print()
                    print(f"Invalid alias format, can only contain a single '=': {a}")
                    sys.exit(1)

                aliases.setdefault(tokens[0], []).append(tokens[1])

    output_details = OutputDetails.from_data(output)
    silent = False
    if output_details.format == "silent":
        silent = True

    force_overwrite = output_details.config.get("force", False)

    target_file: Optional[Path] = None
    if output_details.target != "terminal":
        if output_details.target == "file":
            target_dir = Path.cwd()
            target_file = target_dir / f"{module_or_operation}.{output_details.format}"
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

    kiara_obj: Kiara = ctx.obj["kiara"]

    # =========================================================================
    # prepare manifest
    if module_or_operation in kiara_obj.operations_mgmt.operation_ids:

        operation = kiara_obj.operations_mgmt.get_operation(module_or_operation)
        if module_config:
            print(
                f"Specified run target '{module_or_operation}' is an operation, additional module configuration is not allowed."
            )

    elif module_or_operation in kiara_obj.module_type_names:

        manifest = Manifest(
            module_type=module_or_operation, module_config=module_config
        )

        module = kiara_obj.create_module(manifest=manifest)

        operation = Operation.create_from_module(module)

    elif os.path.isfile(module_or_operation):
        raise NotImplementedError()
        # module_name = kiara_obj.register_pipeline_description(
        #     module_or_operation, raise_exception=True
        # )
    else:
        rich_print(
            f"\nInvalid run target name '[i]{module_or_operation}[/i]'. Must be a path to a pipeline file, or one of the available modules/operations:\n"
        )

        merged = list(kiara_obj.module_type_names)
        merged.extend(kiara_obj.operations_mgmt.operation_ids)
        for n in sorted(merged):
            rich_print(f"  - [i]{n}[/i]")
        sys.exit(1)

    # =========================================================================
    # check save user input
    if save:
        op_output_names = operation.outputs_schema.keys()
        final_aliases = {}
        invalid_fields = []
        for field_name, alias in aliases.items():
            if field_name not in op_output_names:
                invalid_fields.append(field_name)
            else:
                final_aliases[field_name] = alias

        for alias in full_aliases:
            for field_name in op_output_names:
                final_aliases.setdefault(field_name, []).append(f"{alias}-{field_name}")

        if invalid_fields:
            print()
            print(
                f"Can't run workflow, invalid field name(s) when specifying aliases: {', '.join(invalid_fields)}. Valid field names: {', '.join(op_output_names)}"
            )
            sys.exit(1)

    # =========================================================================
    # prepare inputs
    list_keys = []
    for name, value_schema in operation.operation_details.inputs_schema.items():
        if value_schema.type in ["array", "list"]:
            list_keys.append(name)

    inputs_dict = dict_from_cli_args(*inputs, list_keys=list_keys)

    job_config = operation.prepare_job_config(kiara=kiara_obj, inputs=inputs_dict)

    # =========================================================================
    # execute job

    job_id = kiara_obj.job_registry.execute_job(job_config=job_config)
    status = kiara_obj.job_registry.get_job_status(job_id=job_id)

    if status == JobStatus.FAILED:
        job = kiara_obj.job_registry.get_job_details(job_id=job_id)
        print(f"Job failed: {job.error}")
        sys.exit(1)

    outputs = kiara_obj.job_registry.retrieve_result(job_id)

    outputs = operation.process_job_outputs(outputs=outputs)

    print()
    for k, v in outputs.items():
        rendered = kiara_obj.data_registry.render_data(v)
        rich_print(rendered)

    dbg(aliases)
    dbg(full_aliases)
    if save:

        dbg(final_aliases)
        for field_name, aliases in final_aliases.items():
            rich_print(f"Saving '[i]{field_name}[/i]'...")
            try:
                value = outputs.get_value_obj(field_name)
                value.save(aliases=aliases)
                msg = f"   -> done, id: [i]{value.value_id}[/i]"
                if aliases:
                    msg = msg + f", aliases: [i]{', '.join(aliases)}[/i]"
                rich_print(msg)
            except Exception as e:
                if is_debug():
                    import traceback

                    traceback.print_exc()
                rich_print(f"   -> failed: [red]{e}[/red]")
            print()


# @click.argument("module", nargs=1, metavar="MODULE_OR_OPERATION")
# @click.argument("inputs", nargs=-1, required=False)
# @click.option(
#     "--module-render_config",
#     "-c",
#     required=False,
#     help="(Optional) module configuration.",
#     multiple=True,
# )
# @click.option(
#     "--explain",
#     "-e",
#     help="Display additional workflow details.",
#     is_flag=True,
# )
# @click.option(
#     "--output", "-o", help="The output format and configuration.", multiple=True
# )
# @click.option(
#     "--save",
#     "-s",
#     help="Save one or several of the outputs of this run. If the argument contains a '=', the format is [output_name]=[alias], if not, the values will be saved as '[alias]-[output_name]'.",
#     required=False,
#     multiple=True,
# )
# @click.pass_context
# def run_old(ctx, module, inputs, module_config, output, explain, save):
#     """Execute a workflow run."""
#
#     if module_config:
#         module_config = dict_from_cli_args(*module_config)
#     else:
#         module_config = {}
#
#     if not save:
#         aliases: typing.Dict[str, typing.List[str]] = {}
#         full_aliases: typing.List[str] = []
#     else:
#         aliases = {}
#         full_aliases = []
#         for a in save:
#             if "=" not in a:
#                 full_aliases.append(a)
#             else:
#                 tokens = a.split("=")
#                 if len(tokens) != 2:
#                     print()
#                     print(f"Invalid alias format, can only contain a single '=': {a}")
#                     sys.exit(1)
#
#                 aliases.setdefault(tokens[0], []).append(tokens[1])
#
#     kiara_obj: Kiara = ctx.obj["kiara"]
#
#     if module in kiara_obj.available_module_types:
#         module_name = module
#     elif module in kiara_obj.operation_mgmt.profiles.keys():
#         op_config = kiara_obj.operation_mgmt.profiles[module]
#         module_name = op_config.module_type
#         module_config = op_config.module_config
#     elif f"core.{module}" in kiara_obj.available_module_types:
#         module_name = f"core.{module}"
#     elif os.path.isfile(module):
#         module_name = kiara_obj.register_pipeline_description(
#             module, raise_exception=True
#         )
#     else:
#         rich_print(
#             f"\nInvalid module name '[i]{module}[/i]'. Must be a path to a pipeline file, or one of the available modules:\n"
#         )
#         for n in kiara_obj.available_module_types:
#             rich_print(f"  - [i]{n}[/i]")
#         sys.exit(1)
#
#     if not inputs:
#
#         module_obj: KiaraModule = _create_module_instance(
#             ctx=ctx, module_type=module_name, module_config=module_config
#         )
#
#         one_required = False
#         for input_name in module_obj.input_names:
#             if module_obj.input_required(input_name):
#                 one_required = True
#                 break
#
#         if one_required:
#
#             inputs_table = create_table_from_field_schemas(
#                 _show_header=True, **module_obj.inputs_schema
#             )
#             print()
#             print(
#                 "No inputs provided, not running the workflow. To run it, provide input following this schema:"
#             )
#             rich_print(inputs_table)
#             sys.exit(0)
#
#     output_details = OutputDetails.from_data(output)
#     silent = False
#     if output_details.format == "silent":
#         silent = True
#
#     force_overwrite = output_details.render_config.get("force", False)
#
#     # SUPPORTED_TARGETS = ["terminal", "file"]
#     # if output_details.target not in SUPPORTED_TARGETS:
#     #     print()
#     #     rich_print(f"Invalid output target '{output_details.target}', must be one of: [i]{', '.join(SUPPORTED_TARGETS)}[/i]")
#     #     sys.exit(1)
#
#     target_file: typing.Optional[Path] = None
#     if output_details.target != "terminal":
#         if output_details.target == "file":
#             target_dir = Path.cwd()
#             target_file = target_dir / f"{module_name}.{output_details.format}"
#         else:
#             target_file = Path(
#                 os.path.realpath(os.path.expanduser(output_details.target))
#             )
#
#         if target_file.exists() and not force_overwrite:
#             print()
#             print(
#                 f"Can't run workflow, the target files already exist, and '--output force=true' not specified: {target_file}"
#             )
#             sys.exit(1)
#
#     # from kiara.processing.parallel import ThreadPoolProcessor
#     # processor = ThreadPoolProcessor()
#     processor = None
#     controller = BatchController(
#         processor=processor, auto_process=False, kiara=kiara_obj
#     )
#
#     # TODO: should we let the user specify?
#     workflow_id = None
#     if workflow_id is None:
#         workflow_id = f"{module_name}_0"
#
#     workflow = kiara_obj.create_workflow(
#         module_name,
#         module_config=module_config,
#         workflow_id=workflow_id,
#         controller=controller,
#     )
#
#     if save:
#         final_aliases = {}
#         invalid_fields = []
#         for alias, alias in aliases.items():
#             if alias not in workflow.outputs.get_all_field_names():
#                 invalid_fields.append(alias)
#             else:
#                 final_aliases[alias] = alias
#
#         for alias in full_aliases:
#             for alias in workflow.outputs.get_all_field_names():
#                 final_aliases.setdefault(alias, []).append(f"{alias}-{alias}")
#
#         if invalid_fields:
#             print()
#             print(
#                 f"Can't run workflow, invalid field name(s) when specifying aliases: {', '.join(invalid_fields)}. Valid field names: {', '.join(workflow.outputs.get_all_field_names())}"
#             )
#             sys.exit(1)
#
#     list_keys = []
#
#     for name, value in workflow.inputs.items():
#         if value.value_schema.type in ["array", "list"]:
#             list_keys.append(name)
#
#     workflow_input = dict_from_cli_args(*inputs, list_keys=list_keys)
#
#     try:
#         if workflow_input:
#             workflow.inputs.set_values(**workflow_input)
#
#     except Exception as e:
#         import traceback
#
#         traceback.print_exc()
#         print()
#         rich_print(f"[bold red]Input value error[/bold red]: {e}")
#         sys.exit(1)
#
#     try:
#         workflow.controller.process_pipeline()
#     except Exception as e:
#         print()
#         print(e)
#
#     if explain:
#         print()
#         kiara_obj.explain(workflow.current_state)
#
#         if workflow.status == StepStatus.RESULTS_READY:
#             vi = ValuesInfo(workflow.outputs)
#             vi_table = vi.create_value_info_table(
#                 ensure_metadata=True, show_headers=True
#             )
#             panel = Panel(Panel(vi_table), box=box.SIMPLE)
#             rich_print("[b]Output data details[/b]")
#             rich_print(panel)
#
#     if workflow.status != StepStatus.RESULTS_READY:
#
#         failed = {}
#         for step_id in workflow.pipeline.step_ids:
#             job = workflow.controller.get_job_details(step_id)
#             if not job:
#                 continue
#             if job.status == JobStatus.FAILED:
#                 failed[step_id] = job.error if job.error else "-- no error details --"
#
#         print()
#         if failed:
#             rich_print(
#                 "[bold red]Error:[/bold red] One or several workflow steps failed!\n"
#             )
#             for s_id, msg in failed.items():
#                 rich_print(f" - [bold]{s_id}[/bold]: {msg}")
#
#             sys.exit(3)
#         else:
#             rich_print(
#                 f"Workflow results not ready: {', '.join(workflow.outputs.check_invalid().keys())} output(s) missing or invalid"
#             )
#             sys.exit(3)
#     else:
#         if not silent:
#
#             if output_details.target == "terminal":
#                 if output_details.format == "terminal":
#                     print()
#                     all_renderables = []
#
#                     for alias, value in workflow.outputs.items():
#                         try:
#                             renderables = kiara_obj.pretty_print(value, "renderables")
#                         except Exception as e:
#                             if is_debug():
#                                 print(e)
#                             renderables = [str(value.get_value_data())]
#
#                         if isinstance(renderables, str) or not isinstance(
#                             renderables, typing.Iterable
#                         ):
#                             renderables = [renderables]
#                         p = Panel(
#                             RenderGroup(*renderables),
#                             box=box.ROUNDED,
#                             title=f"ValueOrm: [b i]{alias}[/b i]",
#                             title_align="left",
#                         )
#                         all_renderables.append(p)
#
#                     output = Panel(RenderGroup(*all_renderables), box=box.SIMPLE)
#                     rich_print("[b]Output data[/b]")
#                     rich_print(output)
#
#                 else:
#                     raise NotImplementedError()
#
#                     format = output_details.format
#                     available_formats = kiara_obj.get_convert_target_types(
#                         source_type="value_set"
#                     )
#                     if format not in available_formats:
#                         print()
#                         print(
#                             f"Can't convert to output format '{format}', this format is not supported. Available formats: {', '.join(available_formats)}."
#                         )
#                         sys.exit(1)
#
#                     render_config = {}
#                     render_config.update(DEFAULT_TO_JSON_CONFIG)
#
#                     try:
#                         transformed: ValueSet = kiara_obj.transform_data(
#                             workflow.outputs,
#                             source_type="value_set",
#                             target_type=format,
#                             render_config=render_config,
#                         )
#                         transformed_value = transformed.get_value_data("target_value")
#
#                         if format in ["json", "yaml"]:
#                             transformed_str = Syntax(
#                                 transformed_value,
#                                 lexer_name=format,
#                                 background_color="default",
#                             )
#                             rich_print(transformed_str)
#                         else:
#                             print(transformed_value)
#                     except Exception as e:
#                         print()
#                         rich_print(f"Can't transform outputs into '{format}': {e}")
#                         sys.exit(1)
#
#             else:
#                 raise NotImplementedError()
#                 #
#                 # if output_details.format == "terminal":
#                 #
#                 #     renderables = kiara_obj.pretty_print(value=value)
#                 #     output = Panel(RenderGroup(*renderables), box=box.SIMPLE)
#                 #     with open(target_file, "wt") as f:
#                 #         console = Console(record=True, file=f)
#                 #         console.print(output)
#                 # else:
#                 #
#                 #     format = output_details.format
#                 #     available_formats = kiara_obj.get_convert_target_types(
#                 #         source_type="value_set"
#                 #     )
#                 #     if format not in available_formats:
#                 #         print()
#                 #         print(
#                 #             f"Can't convert to output format '{format}', this format is not supported. Available formats: {', '.join(available_formats)}."
#                 #         )
#                 #         sys.exit(1)
#                 #
#                 #     render_config = {}
#                 #     render_config.update(DEFAULT_TO_JSON_CONFIG)
#                 #
#                 #     transformed = kiara_obj.transform_data(
#                 #         workflow.outputs,
#                 #         source_type="value_set",
#                 #         target_type=format,
#                 #         render_config=render_config,
#                 #     )
#                 #     transformed_value = transformed.get_value_data()
#                 #
#                 #     target_file.parent.mkdir(parents=True, exist_ok=True)
#                 #     # TODO: check whether to write text or bytes
#                 #     target_file.write_text(transformed_value)
#
#         if save:
#
#             for alias, aliases in final_aliases.items():
#                 rich_print(f"Saving '[i]{alias}[/i]'...")
#                 try:
#                     value = workflow.outputs.get_value_obj(alias)
#                     value_md = value.save(aliases=aliases)
#                     msg = f"   -> done, id: [i]{value_md.id}[/i]"
#                     if aliases:
#                         msg = msg + f", aliases: [i]{', '.join(aliases)}[/i]"
#                     rich_print(msg)
#                 except Exception as e:
#                     if is_debug():
#                         import traceback
#
#                         traceback.print_exc()
#                     rich_print(f"   -> failed: [red]{e}[/red]")
#                 print()
