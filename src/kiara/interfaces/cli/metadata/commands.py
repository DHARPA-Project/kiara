# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
import rich_click as click
import sys
from rich.panel import Panel

from kiara import Kiara
from kiara.utils import rich_print


@click.group()
@click.pass_context
def metadata(ctx):
    """Metadata-related sub-commands."""


@metadata.command(name="list")
@click.option(
    "--format",
    "-f",
    help="The output format. Defaults to 'terminal'.",
    type=click.Choice(["terminal", "json", "html"]),
    default="terminal",
)
@click.pass_context
def list_metadata(ctx, format):
    """List available metadata schemas."""

    kiara_obj: Kiara = ctx.obj["kiara"]
    metadata_types = kiara_obj.context_info.metadata_types

    if format == "terminal":
        rich_print()
        rich_print(metadata_types)
    elif format == "json":
        print(metadata_types.json(option=orjson.OPT_INDENT_2))
    elif format == "html":
        print(metadata_types.create_html())


@metadata.command(name="explain")
@click.argument("metadata_key", nargs=1, required=True)
@click.option(
    "--format",
    "-f",
    help="The output format. Defaults to 'terminal'.",
    type=click.Choice(["terminal", "html", "json", "json-schema"]),
    default="terminal",
)
@click.option(
    "--details",
    "-d",
    help="Print more metadata schema details (for 'terminal' format).",
    is_flag=True,
)
@click.pass_context
def explain_metadata(ctx, metadata_key, format, details):
    """Print details for a specific metadata schema."""

    kiara_obj: Kiara = ctx.obj["kiara"]
    metadata_types = kiara_obj.context_info.metadata_types

    if metadata_key not in metadata_types.keys():
        print()
        print(f"No metadata schema for key '{metadata_key}' found...")
        sys.exit(1)

    info_obj = metadata_types[metadata_key]

    if format == "terminal":
        rich_print()
        config = {"include_schema": details}
        renderable = info_obj.create_renderable(**config)
        rich_print(
            Panel(
                renderable,
                title=f"Metadata details for: [b]{metadata_key}[/b]",
                title_align="left",
            )
        )
    elif format == "json":
        json_schema = info_obj.json(option=orjson.OPT_INDENT_2)
        print(json_schema)
    elif format == "json-schema":
        json_schema = info_obj.schema_json(option=orjson.OPT_INDENT_2)
        print(json_schema)
    elif format == "html":
        print(info_obj.create_html())
