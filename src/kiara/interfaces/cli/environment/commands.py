# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import click

from kiara.environment import RuntimeEnvironmentMgmt


@click.group(name="environment")
@click.pass_context
def env_group(ctx):
    """Runtime environment-related sub-commands."""


@env_group.command()
@click.pass_context
def list(ctx):
    """List available runtime environment information."""

    kiara_obj = ctx.obj["kiara"]

    remgmt = RuntimeEnvironmentMgmt(kiara=kiara_obj)

    import pp

    pp(remgmt.full_model.dict())

    json_str = remgmt.full_model.schema_json(indent=2)

    print(len(json_str))
