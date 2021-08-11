# -*- coding: utf-8 -*-
import asyncclick as click
import sys

from kiara import Kiara
from kiara.metadata import MetadataSchemaInfo, MetadataSchemasInfo
from kiara.utils.class_loading import find_all_metadata_schemas


@click.group()
@click.pass_context
def metadata(ctx):
    """Metadata-related sub-commands."""


@metadata.command(name="list")
@click.pass_context
def list_metadata(ctx):
    """List available metadata schemas."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    schemas = kiara_obj.metadata_mgmt.all_schemas

    info = MetadataSchemasInfo(metadata_schemas=schemas)
    kiara_obj.explain(info)


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

    kiara_obj: Kiara = ctx.obj["kiara"]

    schemas = find_all_metadata_schemas()
    if metadata_key not in schemas.keys():
        print()
        print(f"No metadata schema for key '{metadata_key}' found...")
        sys.exit(1)

    if not json_schema:
        info = MetadataSchemaInfo(schemas[metadata_key], display_schema=details)
        print()
        kiara_obj.explain(info)
    else:
        print(schemas[metadata_key].schema_json(indent=2))
