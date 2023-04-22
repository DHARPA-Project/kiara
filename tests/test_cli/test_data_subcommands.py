# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from click.testing import CliRunner

from kiara.context import Kiara
from kiara.interfaces.cli import cli


def test_data_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "data")
    assert result.exit_code == 0
    assert "Print the metadata" in result.stdout


def _run_command(kiara_ctx: Kiara, cmd):

    config_path = kiara_ctx.context_config._context_config_path

    cmd = f"--context '{config_path}' {cmd}"
    print(f"Running command:\n\nkiara {cmd}")  # noqa

    runner = CliRunner()
    result = runner.invoke(cli, cmd)
    return result


def test_data_list_subcommand(presseeded_data_store_minimal: Kiara):

    command = "data list --all-values --format json"
    result = _run_command(kiara_ctx=presseeded_data_store_minimal, cmd=command)

    assert result.exit_code == 0
    assert "preseed_minimal.create_table_from_files__table" in result.stdout


def test_data_load_subcommand(presseeded_data_store_minimal: Kiara):

    cmd = "data load alias:preseed_minimal.import_file__file"
    result = _run_command(kiara_ctx=presseeded_data_store_minimal, cmd=cmd)

    assert "Psychiatrische en neurologische bladen" in result.stdout
    assert "City" in result.stdout


def test_data_explain_subcommand(presseeded_data_store_minimal: Kiara):

    cmd = "data explain alias:preseed_minimal.create_table_from_files__table -p"

    result = _run_command(kiara_ctx=presseeded_data_store_minimal, cmd=cmd)

    assert "Latitude" in result.stdout
    assert "type_name" in result.stdout


def test_data_explain_subcommand_2(preseeded_data_store: Kiara):

    cmd = "data explain alias:preseed.journal_nodes_table"

    result = _run_command(kiara_ctx=preseeded_data_store, cmd=cmd)

    assert result.exit_code == 0

    assert "table" in result.stdout


def test_data_load_subcommand_3(preseeded_data_store: Kiara):

    cmd = "data load alias:preseed.journal_nodes_table"
    result = _run_command(kiara_ctx=preseeded_data_store, cmd=cmd)

    assert result.exit_code == 0
    assert "Id" in result.stdout
    assert "City" in result.stdout
