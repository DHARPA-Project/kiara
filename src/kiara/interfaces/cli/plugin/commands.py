# -*- coding: utf-8 -*-
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
from pathlib import Path
from typing import Union

import rich_click as click
from boltons.strutils import slugify

from kiara.utils.cli import terminal_print


@click.group("plugin")
@click.pass_context
def plugin_group(ctx):
    """Kiara plugin related sub-commands."""


@plugin_group.command(name="create")
@click.argument("name", nargs=1, required=False)
@click.option(
    "--path",
    "-p",
    help="The path of the plugin. If not provided, the current working directory will be used.",
    required=False,
)
@click.option("--template", "-t", help="The template to use.", required=False)
@click.pass_context
def create_plugin(ctx, name: str, path: Union[None, str], template: Union[None, str]):
    """Create a new kiara plugin."""

    import sys

    from copier import run_copy

    if not path:
        _path = Path(os.getcwd())
    else:
        _path = Path(path)

    if not _path.exists():
        _path.mkdir(parents=True)

    if not _path.is_dir():
        raise Exception(f"Path '{path}' is not a directory.")

    data = {}

    if name:
        slug = slugify(name)
        if not slug.startswith("kiara_plugin."):
            project_dir = f"kiara_plugin.{slug}"
        else:
            project_dir = slug

        full_path = _path / project_dir

        data["project_name"] = name
        data["project_slug"] = slug
    elif path:
        full_path = _path
    else:
        terminal_print("No name or path provided, please specify at least one of them.")
        sys.exit(1)

    if not template:
        template = "gh:DHARPA-Project/kiara_plugin_template.git"
    run_copy(template, full_path, data=data)

    terminal_print(f"Created new plugin '{name}' in: {full_path}")
