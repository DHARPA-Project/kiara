# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# def test_run_without_module():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "run")
#     assert result.exit_code == 2
#     assert "Missing argument" in result.stdout
#
#
# def test_run_without_args():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "run logic.and")
#     print(result.stdout)
#     assert result.exit_code == 0
#     assert "No inputs provided" in result.stdout
#
#
# def test_run_with_missing_arg():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "run logic.and a=true")
#     assert result.exit_code == 3
#     assert "inputs not ready yet" in result.stdout
#
#
# def test_run_with_valid_inputs():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "run logic.and a=true b=true")
#     assert result.exit_code == 0
#     assert "True" in result.stdout
#
#
# def test_run_with_save():
#
#     runner = CliRunner(env={"KIARA_DATA_STORE": "/tmp/kiara_save_test"})
#     runner.invoke(cli, "data clear-data-store")
#     result = runner.invoke(cli, "run logic.and a=true b=true --save test_save")
#     assert result.exit_code == 0
#     assert "True" in result.stdout
#
#     result_data = runner.invoke(cli, "data list")
#     assert "test_save-y" in result_data.stdout
