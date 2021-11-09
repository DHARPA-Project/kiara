# -*- coding: utf-8 -*-
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
