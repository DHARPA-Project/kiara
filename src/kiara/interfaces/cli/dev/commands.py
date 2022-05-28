# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click

# from alembic import command  # type: ignore
# from alembic.config import Config  # type: ignore
from kiara import Kiara

# noqa
# type: ignore


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


@dev_group.command("test")
@click.pass_context
def test(ctx):

    kiara: Kiara = ctx.obj["kiara"]

    print(kiara.runtime_config)

    # kiara: Kiara = ctx.obj["kiara"]
    #
    # value = kiara.data_registry.get_value("alias:network_data")
    # vl = ValueLineage(kiara=kiara, value=value)
    #
    # terminal_print(vl)


# @dev_group.command("reinit-db")
# @click.pass_context
# def reinit_db(ctx, new_version: Optional[str] = None):
#
#     # os.remove(KIARA_DB_FILE)
#
#     kiara_obj: Kiara = ctx.obj["kiara"]
#
#     script_location = os.path.abspath(KIARA_DB_MIGRATIONS_FOLDER)
#     dsn = kiara_obj._config.db_url
#
#     print(script_location)
#     print(dsn)
#     print("Running DB migrations in %r on %r", script_location, dsn)
#     alembic_cfg = Config(KIARA_DB_MIGRATIONS_CONFIG)
#     alembic_cfg.set_main_option("script_location", script_location)
#     alembic_cfg.set_main_option("sqlalchemy.url", dsn)
#     command.upgrade(alembic_cfg, "head")
