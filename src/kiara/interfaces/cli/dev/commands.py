# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click

# noqa
# type: ignore
from kiara import Kiara

# from alembic import command  # type: ignore
# from alembic.config import Config  # type: ignore


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


@dev_group.command("test")
@click.pass_context
def test(ctx):

    kiara: Kiara = ctx.obj["kiara"]
    # value = kiara.data_registry.register_data(100, schema="integer")
    value = kiara.data_registry.register_data("100", schema="string")
    pp_result = kiara.data_registry.render_data(value_id=value.value_id)
    print(pp_result)

    # DATA_FOLDER = "/home/markus/projects/kiara_new/kiara/examples/data"
    # PIPELINES_FOLDER = "/home/markus/projects/kiara_new/kiara/tests/resources/pipelines"
    #
    # instance_path = "/tmp/kiara_tests"
    # shutil.rmtree(instance_path, ignore_errors=True)
    #
    # kiara = Kiara.create_in_path(instance_path)
    #
    # pipeline = os.path.join(PIPELINES_FOLDER, "test_preseed_1.yaml")
    # batch_op = BatchOperation.from_file(pipeline, kiara=kiara)

    # inputs = {
    #     "edges_file_path": os.path.join(DATA_FOLDER, "journals/JournalEdges1902.csv"),
    #     "nodes_file_path": os.path.join(DATA_FOLDER, "journals/JournalNodes1902.csv"),
    #     "journals_folder_path": os.path.join(DATA_FOLDER, "journals"),
    #     "text_corpus_folder_path": os.path.join(DATA_FOLDER, "text_corpus"),
    #     "city_column_name": "City",
    #     "label_column_name": "Label",
    # }
    #
    # results = batch_op.run(save="journals_data")
    # terminal_print(result)


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
