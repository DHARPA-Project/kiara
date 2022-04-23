# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from click.testing import CliRunner

from kiara.interfaces.cli import cli


def test_operation_list_subcommand():

    runner = CliRunner()

    result = runner.invoke(cli, "operation list")

    assert "logic.nand" in result.stdout


def test_operation_list_by_group_subcommand():

    runner = CliRunner()

    result = runner.invoke(cli, "operation list --by-type")

    assert "logic.nand" in result.stdout


def test_operation_explain_subcommand():

    os_env_vars = {"CONSOLE_WIDTH": "400"}
    runner = CliRunner(env=os_env_vars)

    result = runner.invoke(cli, "operation explain logic.nand")

    print(result.stdout)
    assert "A boolean describing this input state" in result.stdout
    # assert "relation_name" in result.stdout
    # assert "Module metadata" in result.stdout
