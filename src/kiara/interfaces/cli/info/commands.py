# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import rich_click as click
import sys

from kiara.info.kiara import KiaraContext
from kiara.utils.output import rich_print


@click.command()
@click.argument("topic", nargs=1, required=False)
@click.option("--ignore-errors", "-i", help="Ignore errors.", is_flag=True)
@click.option(
    "--format",
    "-f",
    help="The format of the output.",
    type=click.Choice(["terminal", "json", "html"]),
    default="terminal",
)
@click.option(
    "--json-schema",
    "-s",
    help="Print the (json) schema of the output of this command. Don't print the actual info.",
    is_flag=True,
)
@click.pass_context
def info(ctx, topic, ignore_errors, format, json_schema):
    """kiara context information"""

    kiara_obj = ctx.obj["kiara"]

    if json_schema:
        schema = KiaraContext.schema_json(indent=2)
        # s = Syntax(schema, "json", background_color="default")  # doesn't seem to work with pipes
        print(schema)
        sys.exit(0)

    info = KiaraContext.get_info(
        kiara=kiara_obj, sub_type=topic, ignore_errors=ignore_errors
    )

    if format == "terminal":
        rich_print(info)
    elif format == "json":
        print(info.json(indent=2))
    elif format == "html":
        print(info.create_html())
    else:
        print(f"[red]Invalid output format: '{format}'.")
        sys.exit(1)

    sys.exit(0)
