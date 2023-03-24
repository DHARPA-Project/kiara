# -*- coding: utf-8 -*-
from click.testing import CliRunner

from kiara.interfaces.cli import cli

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_non_arg_cli():

    runner = CliRunner()
    result = runner.invoke(cli)
    result.exit_code == 0
    assert "Module-related sub-commands." in result.stdout
    assert "Operation-related sub-commands" in result.stdout

    result_2 = runner.invoke(cli, "--help")
    assert result.stdout == result_2.stdout


# def test_explain_subcommand():
#
#     runner = CliRunner()
#
#     result = runner.invoke(cli, "explain table.query.sql")
#     assert "table.query.sql" in result.stdout
#
#     result = runner.invoke(cli, "explain table")
#     assert "Load a column" in result.stdout
#     assert "Generate a data profile" in result.stdout
