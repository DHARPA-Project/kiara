# -*- coding: utf-8 -*-
import json
from click.testing import CliRunner

from kiara.interfaces.cli import cli


def test_info():

    runner = CliRunner()

    result = runner.invoke(cli, "info --json")

    assert "table.query.sql" in result.stdout
    json.loads(result.stdout)
