# -*- coding: utf-8 -*-

from click.testing import CliRunner

from kiara.interfaces.cli import cli


def test_non_arg_cli():

    runner = CliRunner()
    result = runner.invoke(cli)
    result.exit_code == 0
    assert "Metadata-related sub-commands.\n" in result.stdout

    result_2 = runner.invoke(cli, "--help")
    assert result.stdout == result_2.stdout


def test_explain_subcommand():

    runner = CliRunner()

    result = runner.invoke(cli, "explain table.query.sql")
    assert "table.query.sql" in result.stdout

    result = runner.invoke(cli, "explain table")
    assert "Load a column" in result.stdout
    assert "Generate a data profile" in result.stdout
