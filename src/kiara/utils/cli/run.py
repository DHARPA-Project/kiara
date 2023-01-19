# -*- coding: utf-8 -*-
import sys
import uuid
from click import Context as ClickContext
from pathlib import Path
from pydantic import ValidationError
from rich.console import Group
from rich.markdown import Markdown
from rich.rule import Rule
from typing import Any, Dict, Iterable, List, Mapping, Union

from kiara import KiaraAPI, ValueMap
from kiara.exceptions import (
    FailedJobException,
    KiaraException,
    NoSuchExecutionTargetException,
)
from kiara.interfaces.python_api.utils import create_save_config
from kiara.models.module.operation import Operation

# from kiara.interfaces.python_api.operation import KiaraOperation
from kiara.utils import log_exception
from kiara.utils.cli import dict_from_cli_args, terminal_print
from kiara.utils.cli.rich_click import rich_format_operation_help
from kiara.utils.operations import create_operation_status_renderable
from kiara.utils.output import create_table_from_base_model_cls


def _validate_save_option(save: Iterable[str]) -> bool:

    if save:
        for a in save:
            if "=" in a:
                tokens = a.split("=")
                if len(tokens) != 2:
                    print()
                    print(f"Invalid alias format, can only contain a single '=': {a}")
                    sys.exit(1)
        return True
    else:
        return False


def validate_operation_in_terminal(
    api: KiaraAPI,
    module_or_operation: Union[str, Path, Mapping[str, Any]],
    allow_external=False,
) -> Operation:

    # kiara_op = KiaraOperation(
    #     kiara=kiara,
    #     operation_name=module_or_operation,
    #     operation_config=module_config,
    # )
    try:
        operation = api.get_operation(
            operation=module_or_operation, allow_external=allow_external
        )
        # validate that operation config is valid, ignoring inputs for now
        # kiara_op.operation  # noqa
    except NoSuchExecutionTargetException as nset:
        print()
        terminal_print(nset)
        print()
        print("Existing operations:")
        print()
        for n in nset.avaliable_targets:
            terminal_print(f"  - [i]{n}[/i]")
        sys.exit(1)
    except ValidationError as ve:

        renderables = [""]
        renderables.append("Invalid module configuration:")
        renderables.append("")
        for error in ve.errors():
            loc = ", ".join(error["loc"])  # type: ignore
            renderables.append(f"  [b]{loc}[/b]: [red]{error['msg']}[/red]")

        try:
            if isinstance(module_or_operation, str):
                m = api.context.module_registry.get_module_class(module_or_operation)
                schema = create_table_from_base_model_cls(m._config_cls)
                renderables.append("")
                renderables.append(f"Module configuration schema for '[b i]{m._module_type_name}[/b i]':")  # type: ignore
                renderables.append("")
                renderables.append(schema)
        except Exception:
            pass

        msg = Group(*renderables)
        terminal_print()
        terminal_print(msg, in_panel="[b red]Module configuration error[/b red]")
        sys.exit(1)
    except Exception as e:
        log_exception(e)
        terminal_print()
        terminal_print(
            f"Error when trying to validate the operation [i]'{module_or_operation}'[/i]:\n"
        )
        terminal_print(f"    [red]{e}[/red]")
        root_cause = KiaraException.get_root_details(e)
        if root_cause:
            terminal_print()
            terminal_print(Markdown(root_cause))
        sys.exit(1)

    return operation


def calculate_aliases(
    operation: Operation, alias_tokens: Iterable[str]
) -> Mapping[str, List[str]]:

    if not alias_tokens:
        aliases: Dict[str, List[str]] = {}
        full_aliases: List[str] = []
    else:
        aliases = {}
        full_aliases = []
        for a in alias_tokens:
            if "=" not in a:
                full_aliases.append(a)
            else:
                tokens = a.split("=")
                if len(tokens) != 2:
                    print()
                    print(f"Invalid alias format, can only contain a single '=': {a}")
                    sys.exit(1)

                aliases.setdefault(tokens[0], []).append(tokens[1])

    # =========================================================================
    # check save user input
    final_aliases = {}
    if alias_tokens:
        op_output_names = operation.outputs_schema.keys()
        invalid_fields = []
        for field_name, alias in aliases.items():
            if field_name not in op_output_names:
                invalid_fields.append(field_name)
            else:
                final_aliases[field_name] = alias

        for _alias in full_aliases:
            for field_name in op_output_names:
                final_aliases.setdefault(field_name, []).append(
                    f"{_alias}.{field_name}"
                )

        if invalid_fields:
            print()
            print(
                f"Can't run workflow, invalid field name(s) when specifying aliases: {', '.join(invalid_fields)}. Valid field names: {', '.join(op_output_names)}"
            )
            sys.exit(1)

    return final_aliases


def set_and_validate_inputs(
    api: KiaraAPI,
    operation: Operation,
    inputs: Iterable[str],
    explain: bool,
    print_help: bool,
    click_context: ClickContext,
    cmd_help: str,
) -> ValueMap:

    # =========================================================================
    # prepare inputs
    list_keys = []
    for (
        name,
        value_schema,
    ) in operation.operation_details.inputs_schema.items():
        if value_schema.type in ["list"]:
            list_keys.append(name)

    try:
        inputs_dict = dict_from_cli_args(*inputs, list_keys=list_keys)

        value_map = api.assemble_value_map(
            values=inputs_dict,
            values_schema=operation.inputs_schema,
            register_data=True,
            reuse_existing_data=False,
        )
    except Exception as e:
        terminal_print()
        rg = Group(
            "",
            f"Can't run operation: {e}",
            "",
            Rule(),
            "",
            create_operation_status_renderable(
                operation=operation,
                inputs=None,
                render_config={
                    "show_operation_name": True,
                    "show_inputs": False,
                    "show_outputs_schema": True,
                },
            ),
        )
        terminal_print(rg, in_panel=f"Run info: [b]{operation.operation_id}[/b]")
        sys.exit(1)

    if value_map.check_invalid():
        terminal_print()
        rg = Group(
            "",
            "Can't run operation: invalid or insufficient input(s)",
            "",
            Rule(),
            "",
            create_operation_status_renderable(
                operation=operation,
                inputs=value_map,
                render_config={
                    "show_operation_name": True,
                    "show_inputs": True,
                    "show_outputs_schema": True,
                },
            ),
        )
        terminal_print(rg, in_panel=f"Run info: [b]{operation.operation_id}[/b]")
        sys.exit(1)

    if print_help:
        rich_format_operation_help(
            obj=click_context.command,
            ctx=click_context,
            operation=operation,
            op_inputs=value_map,
            cmd_help=cmd_help,
        )
        sys.exit(0)

    if explain:
        terminal_print()
        rg = Group(
            "",
            create_operation_status_renderable(
                operation=operation,
                inputs=value_map,
                render_config={
                    "show_operation_name": True,
                    "show_inputs": True,
                    "show_outputs_schema": True,
                },
            ),
        )
        terminal_print(rg, in_panel=f"Operation info: [b]{operation.operation_id}[/b]")
        sys.exit(0)

    # try:
    #     operation_inputs = kiara_op.operation_inputs
    # except InvalidValuesException as ive:
    #
    #     terminal_print()
    #     rg = Group(
    #         "",
    #         f"Can't run operation: {ive}",
    #         "",
    #         Rule(),
    #         "",
    #         kiara_op.create_renderable(
    #             show_operation_name=True, show_inputs=True, show_outputs_schema=True
    #         ),
    #     )
    #     terminal_print(rg, in_panel=f"Run info: [b]{kiara_op.operation_name}[/b]")
    #     sys.exit(1)

    if value_map.check_invalid():
        terminal_print()
        rg = Group(
            "",
            "Can't run operation: invalid or insufficient input(s)",
            "",
            Rule(),
            "",
            create_operation_status_renderable(
                operation=operation,
                inputs=value_map,
                render_config={
                    "show_operation_name": True,
                    "show_inputs": True,
                    "show_outputs_schema": True,
                },
            ),
        )
        terminal_print(rg, in_panel=f"Run info: [b]{operation.operation_id}[/b]")
        sys.exit(1)

    if print_help:
        rich_format_operation_help(
            obj=click_context.command,
            ctx=click_context,
            operation=operation,
            op_inputs=value_map,
            cmd_help=cmd_help,
        )
        sys.exit(0)

    return value_map


def execute_job(
    api: KiaraAPI,
    operation: Operation,
    inputs: ValueMap,
    silent: bool,
    save_results: bool,
    aliases: Union[None, Mapping[str, List[str]]],
) -> uuid.UUID:
    """Execute the job"""

    job_id = api.queue_job(operation=operation, inputs=inputs)

    try:
        outputs = api.get_job_result(job_id=job_id)
    except FailedJobException as fje:
        print()
        error: Union[str, None] = KiaraException.get_root_details(fje)
        if not error:
            error = str(fje)
        _error = Markdown(error)
        terminal_print(_error, in_panel="Processing error")

        sys.exit(1)
    except Exception as e:
        print()
        terminal_print(e)
        sys.exit(1)

    if not silent:
        if len(outputs) > 1:
            title = "[b]Results[/b]"
        else:
            title = "[b]Result[/b]"

        # for field_name, value in outputs.items():
        #     results.append("")
        #     results.append(f"* [b i]{field_name}[/b i]")
        #     results.append(kiara_obj.data_registry.render_data(value.value_id))

        terminal_print(
            outputs, in_panel=title, empty_line_before=True, show_data_type=True
        )

    # for k, v in outputs.items():
    #     rendered = kiara_obj.data_registry.render_data(v)
    #     rich_print(rendered)

    if save_results:
        try:

            alias_map = create_save_config(
                field_names=outputs.field_names, aliases=aliases
            )

            saved_results = api.store_values(outputs, alias_map=alias_map)

            api.context.job_registry.store_job_record(job_id=job_id)

            if len(saved_results) == 1:
                title = "[b]Stored result value[/b]"
            else:
                title = "[b]Stored result values[/b]"
            terminal_print(saved_results, in_panel=title, empty_line_before=True)
        except Exception as e:
            log_exception(e)
            terminal_print(f"[red]Error saving results[/red]: {e}")
            sys.exit(1)

    return job_id
