# -*- coding: utf-8 -*-
import asyncclick as click
import sys
from rich import print as rich_print

from kiara.info.kiara import KiaraInfo


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
