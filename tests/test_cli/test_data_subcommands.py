# -*- coding: utf-8 -*-
import pytest

import asyncclick as click
from asyncclick.testing import CliRunner

from kiara import Kiara
from kiara.interfaces.cli import cli

click.anyio_backend = "asyncio"

pytestmark = pytest.mark.anyio


async def test_data_subcommand():

    runner = CliRunner()
    result = await runner.invoke(cli, "data")
    assert result.exit_code == 0
    assert "Print the metadata" in result.stdout


async def test_data_list_subcommand(presseeded_data_store: Kiara):

    runner = CliRunner()
    result = await runner.invoke(
        cli,
        "data list --all",
        env={"KIARA_DATA_STORE": presseeded_data_store.config.data_store},
    )

    assert result.exit_code == 0
    assert "journal_nodes_table" in result.stdout
    assert "journal_nodes@1" in result.stdout


# async def test_data_explain_subcommand(presseeded_data_store: Kiara):
#
#     runner = CliRunner()
#     result = await runner.invoke(
#         cli,
#         "data explain journal_nodes",
#         env={"KIARA_DATA_STORE": presseeded_data_store.config.data_store},
#     )
#     assert result.exit_code == 0
#
#     assert "table" in result.stdout
#
#
# async def test_data_load_subcommand(presseeded_data_store: Kiara):
#
#     runner = CliRunner()
#     result = await runner.invoke(
#         cli,
#         "data load journal_nodes",
#         env={"KIARA_DATA_STORE": presseeded_data_store.config.data_store},
#     )
#     assert result.exit_code == 0
#     assert "Id" in result.stdout
#     assert "City" in result.stdout
