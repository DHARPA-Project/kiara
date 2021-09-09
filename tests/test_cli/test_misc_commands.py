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
