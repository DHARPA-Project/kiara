# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from click.testing import CliRunner

from kiara.interfaces.cli import cli


def test_context_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "context")
    assert result.exit_code == 0
    assert "context related sub-commands" in result.stdout


# def test_delete_context_subcommand():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "module list")
#
#     assert result.exit_code == 0
#     assert "logic.or" in result.stdout
#     assert "logic.xor" not in result.stdout
