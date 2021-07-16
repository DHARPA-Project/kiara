# -*- coding: utf-8 -*-
import asyncclick as click

from kiara import Kiara


@click.group()
@click.pass_context
def operation(ctx):
    """Metadata-related sub-commands."""


@operation.command(name="list")
@click.pass_context
def list_operations(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    profiles = kiara_obj._operation_mgmt.operations

    for p, c in profiles.items():
        print("===============")
        print(p)
        for k, v in c.items():
            print("-----------")
            print("  " + k)
            for v1, v2 in v.items():
                pass

                print("     " + v1)
                # pp(v2)
