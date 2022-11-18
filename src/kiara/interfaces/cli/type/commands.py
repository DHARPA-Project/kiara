# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Type-related subcommands for the cli."""

import rich_click as click
from typing import Dict, Iterable, Type

from kiara.context import Kiara
from kiara.data_types import DataType
from kiara.interfaces.python_api.models.info import (
    DataTypeClassesInfo,
    DataTypeClassInfo,
)
from kiara.utils.cli import output_format_option, terminal_print_model
from kiara.utils.graphs import print_ascii_graph


@click.group(name="data-type")
@click.pass_context
def type_group(ctx):
    """Information about available data types."""


@type_group.command(name="list")
@click.option(
    "--full-doc",
    "-d",
    is_flag=True,
    help="Display the full documentation for every data type (when using 'terminal' output format).",
)
@click.option(
    "--include-internal",
    "-I",
    is_flag=True,
    help="Also list types that are only (or mostly) used internally.",
)
@click.argument("filter", nargs=-1, required=False)
@output_format_option()
@click.pass_context
def list_types(
    ctx, full_doc, include_internal: bool, filter: Iterable[str], format: str
):
    """List available data_types."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    if not include_internal:
        type_classes: Dict[str, Type[DataType]] = {}
        for name, cls in kiara_obj.data_type_classes.items():
            if not kiara_obj.type_registry.is_internal_type(name):
                type_classes[name] = cls
    else:
        type_classes = dict(kiara_obj.data_type_classes)

    title = "Available data types"
    if filter:
        title = "Filtered data types"
        temp = {}
        for k, v in type_classes.items():
            match = True
            for f in filter:
                if f.lower() not in k.lower():
                    match = False
                    break
            if match:
                temp[k] = v
        type_classes = temp

    data_types_info = DataTypeClassesInfo.create_from_type_items(
        kiara=kiara_obj, group_title=title, **type_classes
    )

    terminal_print_model(
        data_types_info, format=format, in_panel="Available data types"
    )


@type_group.command(name="hierarchy")
@click.option(
    "--include-internal",
    "-i",
    is_flag=True,
    help="Display internally used data types.",
    default=False,
)
@click.pass_context
def hierarchy(ctx, include_internal) -> None:
    """Show the current runtime environments' type hierarchy."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    type_mgmt = kiara_obj.type_registry
    print()

    if include_internal:
        print_ascii_graph(type_mgmt.data_type_hierarchy)
    else:
        sub_graph = type_mgmt.get_sub_hierarchy("any")
        print_ascii_graph(sub_graph)


@type_group.command(name="explain")
@click.argument("type_name", nargs=1, required=True)
@output_format_option()
@click.pass_context
def explain_data_type(ctx, type_name: str, format: str):
    """Print details of a data type."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    data_type = kiara_obj.type_registry.retrieve_data_type(
        data_type_name=type_name, data_type_config=None
    )

    instance_renderable = data_type.create_renderable(show_type_info=False)
    type_renderable = DataTypeClassInfo.create_from_type_class(
        type_cls=data_type.__class__, kiara=kiara_obj
    )

    terminal_print_model(
        instance_renderable,
        type_renderable,
        format=format,
        in_panel=f"Data type: [b i]{data_type.data_type_name}[/b i]",
    )
