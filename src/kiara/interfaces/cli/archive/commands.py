# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from typing import TYPE_CHECKING

import rich_click as click

from kiara.utils.cli import (
    OutputFormat,
    output_format_option,
    terminal_print_model,
)

if TYPE_CHECKING:
    pass


@click.group("archive")
@click.pass_context
def context(ctx):
    """Kiara archive related sub-commands."""


@context.command("explain")
@click.argument("archive", nargs=1, required=True)
@output_format_option()
@click.pass_context
def explain_archive(
    ctx,
    format: str,
    archive: str,
):
    """Print details of an archive file."""

    from kiara.api import KiaraAPI

    kiara_api: KiaraAPI = ctx.obj.kiara_api

    infos = kiara_api.get_archive_info(archive)

    if not format or format == OutputFormat.TERMINAL:
        for info in infos:
            types = ", ".join(info.archive_type_info.supported_item_types)
            terminal_print_model(info, in_panel=f"Archive type(s): {types}")
    else:
        terminal_print_model(*infos, format=format)
