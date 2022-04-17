# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
import rich_click as click
from alembic import command  # type: ignore
from alembic.config import Config  # type: ignore
from typing import Optional

from kiara.context import Kiara
from kiara.defaults import KIARA_DB_MIGRATIONS_CONFIG, KIARA_DB_MIGRATIONS_FOLDER
from kiara.interfaces.python_api.batch import BatchConfig
from kiara.utils.cli import terminal_print


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


@dev_group.command("test")
@click.pass_context
def test(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    config = BatchConfig.from_file(
        "/home/markus/projects/kiara_new/nand_batch.json", kiara=kiara_obj
    )
    # print(type(config))
    # dbg(config.dict())
    terminal_print(config.structure.pipeline_inputs_schema)
    terminal_print(config.structure.pipeline_outputs_schema)


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
