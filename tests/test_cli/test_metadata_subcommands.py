# -*- coding: utf-8 -*-

from click.testing import CliRunner

from kiara.interfaces.cli import cli


def test_metadata_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "metadata")
    assert result.exit_code == 0
    assert "Print details for a specific" in result.stdout


def test_metadata_list_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "metadata list")

    assert result.exit_code == 0
    assert "table" in result.stdout
    assert "file_bundle" in result.stdout


def test_metadata_explain_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "metadata explain table")

    assert result.exit_code == 0
    assert "'table'" in result.stdout
    assert "column_names" in result.stdout
