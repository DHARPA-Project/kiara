# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# def test_metadata_subcommand():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "metadata")
#     assert result.exit_code == 0
#     assert "Print details for a specific" in result.stdout
#
#
# def test_metadata_list_subcommand():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "metadata list")
#
#     assert result.exit_code == 0
#     assert "python_class" in result.stdout
#
#
# def test_metadata_explain_subcommand():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "metadata explain python_class")
#
#     assert result.exit_code == 0
#     assert "Python class" in result.stdout
#     assert "Markus Binsteiner" in result.stdout
