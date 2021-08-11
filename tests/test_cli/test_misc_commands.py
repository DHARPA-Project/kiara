# -*- coding: utf-8 -*-
import pytest

import asyncclick as click
from asyncclick.testing import CliRunner

from kiara.interfaces.cli import cli

click.anyio_backend = "asyncio"

pytestmark = pytest.mark.anyio


async def test_non_arg_cli():

    runner = CliRunner()
    result = await runner.invoke(cli)
    result.exit_code == 0
    assert "Metadata-related sub-commands.\n" in result.stdout

    result_2 = await runner.invoke(cli, "--help")
    assert result.stdout == result_2.stdout
