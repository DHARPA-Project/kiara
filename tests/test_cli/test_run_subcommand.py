# -*- coding: utf-8 -*-
import os
import sys

import pytest
from click.testing import CliRunner

from kiara.interfaces.cli import cli

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

DATA_FOLDER = os.path.join(ROOT_DIR, "examples", "data")
TEST_RESOURCES_FOLDER = os.path.join(ROOT_DIR, "tests", "resources")
KIARA_CONFIG_FILE = os.path.join(TEST_RESOURCES_FOLDER, "kiara.config")


def test_run_without_module():

    runner = CliRunner()
    result = runner.invoke(cli, "run")
    assert result.exit_code == 2
    assert "Missing argument" in result.stdout


def test_run_without_args():

    runner = CliRunner()
    result = runner.invoke(cli, "run logic.and")
    assert result.exit_code == 1
    assert "invalid or insufficient input" in result.stdout
    assert "not set" in result.stdout


def test_run_with_missing_arg():

    runner = CliRunner()
    result = runner.invoke(cli, "run logic.and a=true")
    assert result.exit_code == 1
    assert "not set" in result.stdout
    assert "valid" in result.stdout
    assert "invalid or insufficient input" in result.stdout


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Config path does not run on Windows for some reason, need to investigate",
)
def test_run_with_valid_inputs():

    runner = CliRunner()
    result = runner.invoke(
        cli,
        f'-cnf {KIARA_CONFIG_FILE} run logic.and a=true b=true --comment "A comment."',
    )
    assert result.exit_code == 0
    assert "True" in result.stdout


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Config path does not run on Windows for some reason, need to investigate",
)
def test_run_with_save():

    runner = CliRunner(env={"KIARA_CONTEXT": "_unit_tests_run"})
    runner.invoke(cli, "context delete -f")
    result = runner.invoke(
        cli,
        f'-cnf {KIARA_CONFIG_FILE} run logic.and a=true b=true --save test_save --comment "A comment."',
    )
    assert result.exit_code == 0
    assert "True" in result.stdout

    result_data = runner.invoke(cli, "data list")
    assert "test_save.y" in result_data.stdout


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Config path does not run on Windows for some reason, need to investigate",
)
def test_run_with_missing_comment():

    runner = CliRunner()
    result = runner.invoke(cli, f"-cnf {KIARA_CONFIG_FILE} run logic.and a=true b=true")
    assert result.exit_code == 1
    assert "No job metadata provided." in result.stdout
