# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from click.testing import CliRunner

from kiara.interfaces.cli import cli


def test_operation_list_subcommand():

    runner = CliRunner()

    result = runner.invoke(cli, "operation list")

    assert "table.query.sql" in result.stdout


def test_operation_list_by_group_subcommand():

    runner = CliRunner()

    result = runner.invoke(cli, "operation list --by-type")

    assert "calculate_hash.sha3_256.for.file" in result.stdout


def test_operation_explain_subcommand():

    runner = CliRunner()

    result = runner.invoke(cli, "operation explain table.query.sql")

    assert "table.query.sql" in result.stdout
    # assert "relation_name" in result.stdout
    # assert "Module metadata" in result.stdout
