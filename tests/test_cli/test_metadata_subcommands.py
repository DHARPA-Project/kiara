# -*- coding: utf-8 -*-
import pytest

import asyncclick as click
from asyncclick.testing import CliRunner

from kiara.interfaces.cli import cli

click.anyio_backend = "asyncio"

pytestmark = pytest.mark.anyio


async def test_metadata_subcommand():

    runner = CliRunner()
    result = await runner.invoke(cli, "metadata")
    assert result.exit_code == 0
    assert "Print details for a specific" in result.stdout


async def test_metadata_list_subcommand():

    runner = CliRunner()
    result = await runner.invoke(cli, "metadata list")

    assert result.exit_code == 0
    assert "table" in result.stdout
    assert "file_bundle" in result.stdout


async def test_metadata_explain_subcommand():

    runner = CliRunner()
    result = await runner.invoke(cli, "metadata explain table")

    assert result.exit_code == 0
    assert "'table'" in result.stdout
    assert "column_names" in result.stdout
