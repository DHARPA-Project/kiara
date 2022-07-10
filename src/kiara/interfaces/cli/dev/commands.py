# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click

# noqa
# type: ignore
from kiara.utils.cli import output_format_option, terminal_print_model
from kiara.utils.develop import KIARA_DEV_SETTINGS, KiaraDevSettings

# from alembic import command  # type: ignore
# from alembic.config import Config  # type: ignore


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


@dev_group.group("config")
@click.pass_context
def config(ctx):
    """Kiara config related sub-commands."""


@config.command("print")
@output_format_option()
@click.pass_context
def print_config(ctx, format: str):

    KIARA_DEV_SETTINGS.dict()
    config: KiaraDevSettings = KIARA_DEV_SETTINGS
    title = "Develop mode configuration"
    terminal_print_model(config, format=format, in_panel=title)


# @dev_group.command("test")
# @click.pass_context
# def test(ctx):
#
#     # kiara: Kiara = ctx.obj["kiara"]
#     def run():
#         command = ["kiara", "data", "list"]
#         command = [
#             "kiara",
#             "run",
#             "examples/pipelines/tutorial_3.yaml",
#             "csv_file_path=examples/data/journals/JournalNodes1902.csv",
#             "filter_string=Amsterdam",
#             "column_name=City",
#         ]
#
#         extra_env = {}
#         os_env_vars = copy.deepcopy(os.environ)
#         _run_env = dict(os_env_vars)
#         if extra_env:
#             _run_env.update(extra_env)
#
#         print(f"RUNNING: {' '.join(command)}")
#         p = Popen(command, stdout=PIPE, stderr=PIPE, env=_run_env)
#         stdout, stderr = p.communicate()
#
#         stdout_str = stdout.decode("utf-8")
#         stderr_str = stderr.decode("utf-8")
#         print("stdout:")
#         print(stdout_str)
#         print("stderr:")
#         print(stderr_str)
#
#     thread = Thread(target=run, daemon=True)
#     thread.run()
#
#     # print(kiara.runtime_config)
#
#     # kiara: Kiara = ctx.obj["kiara"]
#     #
#     # value = kiara.data_registry.get_value("alias:network_data")
#     # vl = ValueLineage(kiara=kiara, value=value)
#     #
#     # terminal_print(vl)


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
