# -*- coding: utf-8 -*-

from click.testing import CliRunner

from kiara.interfaces.cli import cli


def test_module_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "module")
    assert result.exit_code == 0
    assert "Module-related sub-commands" in result.stdout


def test_logic_list_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "module list")

    assert result.exit_code == 0
    assert "logic.xor" in result.stdout
    assert "table.import_from.file_path.string" not in result.stdout


def test_module_explain_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "module explain logic.xor")

    assert result.exit_code == 0
    assert "Pipeline config" in result.stdout
    assert "Processing source code" not in result.stdout
    # assert "https://github.com/DHARPA-Project/kiara_modules.core" in result.stdout

    result = runner.invoke(cli, "module explain logic.and")

    assert result.exit_code == 0
    assert "Pipeline config" not in result.stdout
    assert "Processing source code" in result.stdout
    # assert "https://github.com/DHARPA-Project/kiara_modules.core" in result.stdout


def test_module_explain_instance_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "module explain-instance logic.xor")

    assert result.exit_code == 0
    assert "Inputs" in result.stdout
    assert "constants" in result.stdout

    result = runner.invoke(cli, "module explain-instance logic.and")
    assert result.exit_code == 0
    assert "Inputs" in result.stdout
    assert "constants" in result.stdout
