# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click

from kiara.defaults import (
    CHUNK_COMPRESSION_TYPE,
    DEFAULT_CHUNK_COMPRESSION,
)
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

    from kiara.interfaces.python_api.base_api import BaseAPI

    kiara_api: BaseAPI = ctx.obj.base_api

    info = kiara_api.retrieve_archive_info(archive)

    terminal_print_model(info, format=format, in_panel=f"Archive info: {archive}")


@archive.command("export")
@click.argument("path", nargs=1, required=True)
@click.option(
    "--compression",
    "-c",
    help="The compression inside the archive. If not provided, 'zstd' will be used. Ignored if archive already exists and 'append' is used.",
    type=click.Choice(["zstd", "lz4", "lzma", "none"]),
    default=DEFAULT_CHUNK_COMPRESSION.ZSTD.name.lower(),  # type: ignore
)
@click.option("--append", "-a", help="Append data to existing archive.", is_flag=True)
@click.option("--no-aliases", "-na", help="Do not store aliases.", is_flag=True)
@click.pass_context
@handle_exception()
def export_archive(ctx, path: str, compression: str, append: bool, no_aliases: bool):

    from kiara.interfaces.python_api.base_api import BaseAPI

    api: BaseAPI = ctx.obj.base_api

    target_store_params = {"compression": CHUNK_COMPRESSION_TYPE[compression.upper()]}
    result = api.export_archive(
        target_archive=path,
        append=append,
        target_store_params=target_store_params,
        no_aliases=no_aliases,
    )

    render_config = {"add_field_column": False}
    terminal_print_model(
        result,
        format="terminal",
        empty_line_before=None,
        in_panel="Exported values",
        **render_config,
    )


@archive.command("import")
@click.argument("path", nargs=1, required=True)
@click.option("--no-aliases", "-na", help="Do not store aliases.", is_flag=True)
@click.pass_context
@handle_exception()
def import_archive(ctx, path: str, no_aliases: bool):
    """Import an archive file."""

    from kiara.interfaces.python_api.base_api import BaseAPI

    kiara_api: BaseAPI = ctx.obj.base_api

    result = kiara_api.import_archive(source_archive=path, no_aliases=no_aliases)

    render_config = {"add_field_column": False}
    terminal_print_model(
        result,
        format="terminal",
        empty_line_before=None,
        in_panel="Imported values",
        **render_config,
    )
