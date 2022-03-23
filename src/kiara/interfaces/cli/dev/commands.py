# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
from typing import Optional, Any, Dict

import rich_click as click
from alembic import command
from alembic.config import Config

from kiara import Kiara
from kiara.defaults import KIARA_DB_MIGRATIONS_FOLDER, KIARA_DB_MIGRATIONS_CONFIG
from kiara.models.module.jobs import JobConfig
from kiara.models.module.manifest import Manifest
from kiara.modules.operations.included_core_operations.render_value import RenderValueOperationType
from kiara.utils.output import rich_print


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


@dev_group.command("test")
@click.pass_context
def test(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    op = Manifest(module_type="core.example")

    inputs = {"text_1": "xxxx", "text_2": "yyyy", "aaaa": "bbb"}
    # job = JobConfig(inputs=inputs, **op.dict())
    results = kiara_obj.execute(manifest=op, inputs=inputs)
    text_value = results["text"]

    kiara_obj.data_registry.register_alias("test.test.test3", text_value)

    kiara_obj.data_registry._alias_registry.print_tree()

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
def reinit_db(ctx, new_version: Optional[str]=None):

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
