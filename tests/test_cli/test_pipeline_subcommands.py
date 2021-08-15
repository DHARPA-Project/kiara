# -*- coding: utf-8 -*-
import pytest

import asyncclick as click
import os
from asyncclick.testing import CliRunner

from kiara.interfaces.cli import cli

click.anyio_backend = "asyncio"

pytestmark = pytest.mark.anyio


async def test_pipeline_subcommand():

    runner = CliRunner()
    result = await runner.invoke(cli, "pipeline")
    assert result.exit_code == 0
    assert "Pipeline-related sub-commands" in result.stdout


async def test_pipeline_explain_subcommand():

    runner = CliRunner()
    result = await runner.invoke(cli, "pipeline explain logic.xor")

    assert result.exit_code == 0
    assert "Pipeline structure" in result.stdout
    assert "Processing stage: 2" in result.stdout

    pipeline_file = os.path.join(
        os.path.dirname(__file__),
        "..",
        "resources",
        "pipelines",
        "logic",
        "logic_3.json",
    )
    abs_path = os.path.abspath(pipeline_file)
    result = await runner.invoke(cli, f"pipeline explain {abs_path}")

    assert result.exit_code == 0
    assert "Pipeline structure" in result.stdout
    assert "Processing stage: 2" in result.stdout


async def test_pipeline_explain_steps_subcommand():

    runner = CliRunner()
    result = await runner.invoke(cli, "pipeline explain-steps logic.xor")

    assert result.exit_code == 0
    assert "logic.or" in result.stdout
    assert "logic.nand" in result.stdout
    assert "logic.and" in result.stdout

    pipeline_file = os.path.join(
        os.path.dirname(__file__),
        "..",
        "resources",
        "pipelines",
        "logic",
        "logic_3.json",
    )
    abs_path = os.path.abspath(pipeline_file)
    result = await runner.invoke(cli, f"pipeline explain {abs_path}")

    assert result.exit_code == 0
    assert "logic.and" in result.stdout
