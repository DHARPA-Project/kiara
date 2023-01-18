# -*- coding: utf-8 -*-
import os
from click.testing import CliRunner

from kiara.interfaces.cli import cli

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_pipeline_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "pipeline")
    assert result.exit_code == 0
    assert "Pipeline-related sub-commands" in result.stdout


def test_pipeline_explain_subcommand():

    runner = CliRunner()
    result = runner.invoke(cli, "pipeline explain logic.xor")

    assert result.exit_code == 0
    assert (
        "Returns 'True' if exactly one of it's two inputs is 'True'." in result.stdout
    )
    assert "structure" in result.stdout
    assert "outputs" in result.stdout

    pipeline_file = os.path.join(
        os.path.dirname(__file__),
        "..",
        "resources",
        "pipelines",
        "logic",
        "logic_3.json",
    )
    abs_path = os.path.abspath(pipeline_file)
    result = runner.invoke(cli, f"pipeline explain '{abs_path}'")

    assert result.exit_code == 0
    assert "structure" in result.stdout
    assert "and_1_1__y" in result.stdout
