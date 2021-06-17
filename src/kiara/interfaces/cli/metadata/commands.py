# -*- coding: utf-8 -*-
import asyncclick as click


@click.group()
@click.pass_context
def metadata(ctx):
    """Metadata-related sub-commands."""
