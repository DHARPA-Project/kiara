# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import rich_click as click
import sys

from kiara import Kiara
from kiara.utils.cli import output_format_option, terminal_print_model


@click.group()
@click.pass_context
def metadata(ctx):
    """Metadata-related sub-commands."""


@metadata.command(name="list")
@output_format_option()
@click.pass_context
def list_metadata(ctx, format):
    """List available metadata schemas."""

    kiara_obj: Kiara = ctx.obj["kiara"]
    metadata_types = kiara_obj.context_info.metadata_types

    terminal_print_model(
        metadata_types, format=format, in_panel="Available metadata types"
    )


@metadata.command(name="explain")
@click.argument("metadata_key", nargs=1, required=True)
@click.option(
    "--details",
    "-d",
    help="Print more metadata schema details (for 'terminal' format).",
    is_flag=True,
)
@output_format_option()
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

    terminal_print_model(
        info_obj,
        format=format,
        in_pane=f"Details for metadata type: [b i]{metadata_key}[/b i]",
    )
