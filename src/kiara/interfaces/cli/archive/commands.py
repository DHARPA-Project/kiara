# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click

from kiara.utils.cli import (
    output_format_option,
    terminal_print_model,
)
from kiara.utils.cli.exceptions import handle_exception


@click.group()
@click.pass_context
def archive(ctx):
    """Kiara archive related sub-commands."""


@archive.command("explain")
@click.argument("archive", nargs=1, required=True)
@output_format_option()
@click.pass_context
@handle_exception()
def explain_archive(
    ctx,
    format: str,
    archive: str,
):
    """Print details of an archive file."""

    from kiara.api import KiaraAPI

    kiara_api: KiaraAPI = ctx.obj.kiara_api

    info = kiara_api.retrieve_archive_info(archive)

    terminal_print_model(info, format=format, in_panel=f"Archive info: {archive}")


@archive.command("import")
@click.argument("archive", nargs=1, required=True)
# @click.option(
#     "--all-values",
#     "-a",
#     is_flag=True,
#     default=False,
#     help="Import all values from the archive, even if they don't have an alias associated with them.",
# )
@click.pass_context
@handle_exception()
def import_archive(ctx, archive: str, all_values: bool = False):
    """Import an archive file."""

    from kiara.interfaces.python_api import KiaraAPI

    kiara_api: KiaraAPI = ctx.obj.kiara_api

    result = kiara_api.import_archive(archive)

    render_config = {"add_field_column": False}
    terminal_print_model(result, **render_config)
