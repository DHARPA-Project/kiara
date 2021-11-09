# -*- coding: utf-8 -*-

from click.testing import CliRunner

from kiara.interfaces.cli import cli


def test_run_without_module():

    runner = CliRunner()
    result = runner.invoke(cli, "run")
    assert result.exit_code == 2
    assert "Missing argument" in result.stdout


def test_run_without_args():

    runner = CliRunner()
    result = runner.invoke(cli, "run logic.and")
    print(result.stdout)
    assert result.exit_code == 0
    assert "No inputs provided" in result.stdout


def test_run_with_missing_arg():

    runner = CliRunner()
    result = runner.invoke(cli, "run logic.and a=true")
    assert result.exit_code == 3
    assert "inputs not ready yet" in result.stdout


def test_run_with_valid_inputs():

    runner = CliRunner()
    result = runner.invoke(cli, "run logic.and a=true b=true")
    assert result.exit_code == 0
    assert "True" in result.stdout


def test_run_with_save():

    runner = CliRunner()
    result = runner.invoke(cli, "run logic.and a=true b=true --save test_save")
    assert result.exit_code == 0
    assert "True" in result.stdout

    result_data = runner.invoke(cli, "data list")
    assert "test_save" in result_data.stdout
