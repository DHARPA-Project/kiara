# -*- coding: utf-8 -*-
import click
import sys

import json as _json

from rich.syntax import Syntax

from kiara.info.kiara import KiaraContext
from kiara.utils.output import rich_print


@click.command()
@click.argument("topic", nargs=1, required=False)
@click.option("--ignore-errors", "-i", help="Ignore errors.", is_flag=True)
@click.option("--json", "-j", help="Print the result in json format", is_flag=True)
@click.option("--json-schema", "-s", help="Print the (json) schema of the output of this command.", is_flag=True)
@click.pass_context
def info(ctx, topic, ignore_errors, json, json_schema):
    """kiara context information"""

    kiara_obj = ctx.obj["kiara"]

    if json_schema:
        schema = KiaraContext.schema_json(indent=2)
        # s = Syntax(schema, "json", background_color="default")  # doesn't seem to work with pipes
        print(schema)
        sys.exit(0)

    if not topic:
        info = KiaraContext.create(kiara=kiara_obj, ignore_errors=ignore_errors)
    else:

        if topic not in KiaraContext.__fields__.keys():
            print(
                f"Info topic '{topic}' not available. Available topics: {', '.join(KiaraContext.__fields__.keys())}"
            )
            sys.exit(1)

        _info = KiaraContext.create(kiara=kiara_obj, ignore_errors=ignore_errors)
        info = getattr(_info, topic)

    if json:
        print(info.json(indent=2))
    else:
        rich_print(info)

