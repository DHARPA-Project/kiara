# -*- coding: utf-8 -*-
import click
import sys
from rich.panel import Panel
from rich.syntax import Syntax

from kiara import Kiara
from kiara.info.metadata import MetadataModelsInfo
from kiara.metadata.core_models import MetadataModelMetadata
from kiara.utils.class_loading import find_all_metadata_schemas
from kiara.utils.output import rich_print


@click.group()
@click.pass_context
def metadata(ctx):
    """Metadata-related sub-commands."""


@metadata.command(name="list")
@click.pass_context
def list_metadata(ctx):
    """List available metadata schemas."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    keys = kiara_obj.metadata_mgmt.all_schemas.keys()
    print()
    info = MetadataModelsInfo.from_metadata_keys(*keys, kiara=kiara_obj)
    kiara_obj.explain(
        Panel(info, title="Available metadata schemas", title_align="left")
    )


@metadata.command(name="explain")
@click.argument("metadata_key", nargs=1, required=True)
@click.option(
    "--json-schema",
    "-j",
    help="Only print json schema.",
    is_flag=True,
)
@click.option("--details", "-d", help="Print more schema details.", is_flag=True)
@click.pass_context
def explain_metadata(ctx, metadata_key, json_schema, details):
    """Print details for a specific metadata schema."""

    # kiara_obj: Kiara = ctx.obj["kiara"]

    schemas = find_all_metadata_schemas()
    if metadata_key not in schemas.keys():
        print()
        print(f"No metadata schema for key '{metadata_key}' found...")
        sys.exit(1)

    if not json_schema:
        info = MetadataModelMetadata.from_model_class(schemas[metadata_key])
        print()
        title = f"Metadata schema: [b]{metadata_key}[/b]"
        renderable = Panel(
            info.create_renderable(display_schema=details),
            title=title,
            title_align="left",
        )
    else:
        renderable = Syntax(
            schemas[metadata_key].schema_json(indent=2),
            "json",
            background_color="default",
        )

    rich_print(renderable)
