# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
import rich_click as click
from alembic import command
from alembic.config import Config
from typing import Optional

from kiara.defaults import KIARA_DB_MIGRATIONS_CONFIG, KIARA_DB_MIGRATIONS_FOLDER
from kiara.kiara import Kiara
from kiara.utils import rich_print
from kiara.utils.metadata import find_metadata_models


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


@dev_group.command("test")
@click.pass_context
def test(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    m = find_metadata_models()
    rich_print(m)

    # for value_id in kiara_obj.destiny_registry.all_values:
    #     print("-----")
    #     print(str(value_id))
    #     aliases = kiara_obj.destiny_registry.get_destiny_aliases_for_value(value_id)
    #     for alias in aliases:
    #         print(f"- alias: {alias}")
    #
    #
    # op = Manifest(module_type="core.example")
    #
    # inputs = {"text_1": "xxxx", "text_2": "yyyy", "aaaa": "bbb"}
    # # job = JobConfig(inputs=inputs, **op.dict())
    #
    # op = KiaraOperation(kiara=kiara_obj, operation_name="core.example")
    # op.set_inputs(**inputs)
    #
    # op.queue_job()
    #
    # result = op.retrieve_result()
    #
    # rich_print(result)
    #
    # for field_name in result.field_names:
    #     value = result.get_value_obj(field_name)
    #
    #     if value.is_stored:
    #         continue
    #
    #     op_type: ExtractMetadataOperationType = kiara_obj.operation_registry.get_operation_type("extract_metadata")  # type: ignore
    #     operations = op_type.get_operations_for_data_type(value.value_schema.type)
    #     for metadata_key, op in operations.items():
    #         op_details: ExtractMetadataDetails = op.operation_details  # type: ignore
    #         input_field_name = op_details.input_field_name
    #         result_field_name = op_details.result_field_name
    #         d = kiara_obj.destiny_registry.add_destiny(
    #             destiny_alias=f"metadata.{metadata_key}",
    #             values={input_field_name: value.value_id},
    #             manifest=op,
    #             result_field_name=result_field_name,
    #         )
    #
    #         kiara_obj.destiny_registry.resolve_destiny(d)
    #
    #         kiara_obj.destiny_registry.attach_as_property(d)
    #         # kiara_obj.destiny_registry.store_destiny(d)
    #
    #
    # all_destinies = kiara_obj.destiny_registry.get_destiny_aliases_for_value(value_id=value.value_id)
    #
    # for k, v in result.items():
    #     kiara_obj.data_registry.store_value(v)

    # op.save_result(aliases="test_result")

    # kiara_obj.destiny_registry.store_destiny(destiny_id=destiny.destiny_id)
    # kiara_obj.data_registry.store_value(value=text_value)

    # import pp
    # pp(kiara_obj.data_registry._destinies_by_value)

    # rich_print(kiara_obj.data_registry.default_data_store)

    # op_type: RenderValueOperationType = kiara_obj.operations_mgmt.get_operation_type("render_value")
    # op = op_type.get_operation_for_render_combination(source_type=text_value.value_schema.type, target_type="string")
    #
    # result = op.run(kiara=kiara_obj, inputs={"value": text_value})
    # rendered = result.get_value_data("rendered_value")
    # print(rendered)


@dev_group.command("reinit-db")
@click.pass_context
def reinit_db(ctx, new_version: Optional[str] = None):

    # os.remove(KIARA_DB_FILE)

    kiara_obj: Kiara = ctx.obj["kiara"]

    script_location = os.path.abspath(KIARA_DB_MIGRATIONS_FOLDER)
    dsn = kiara_obj._config.db_url

    print(script_location)
    print(dsn)
    print("Running DB migrations in %r on %r", script_location, dsn)
    alembic_cfg = Config(KIARA_DB_MIGRATIONS_CONFIG)
    alembic_cfg.set_main_option("script_location", script_location)
    alembic_cfg.set_main_option("sqlalchemy.url", dsn)
    command.upgrade(alembic_cfg, "head")
