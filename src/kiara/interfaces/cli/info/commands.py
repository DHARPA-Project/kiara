# -*- coding: utf-8 -*-
import click
import sys

from kiara.info.kiara import KiaraInfo
from kiara.utils.output import rich_print


@click.command()
@click.argument("topic", nargs=1, required=False)
@click.pass_context
def info(ctx, topic):
    """kiara context information"""

    kiara_obj = ctx.obj["kiara"]

    if not topic:
        info = KiaraInfo.create(kiara=kiara_obj)
        rich_print(info)
    else:

        if topic not in KiaraInfo.__fields__.keys():
            print(
                f"Info topic '{topic}' not available. Available topics: {', '.join(KiaraInfo.__fields__.keys())}"
            )
            sys.exit(1)

        info = KiaraInfo.create(kiara=kiara_obj)
        rich_print(getattr(info, topic))
