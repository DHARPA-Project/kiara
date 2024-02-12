# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from typing import TYPE_CHECKING

import rich_click as click

from kiara.utils.cli import (
    output_format_option,
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

    info = kiara_api.get_archive_info(archive)
