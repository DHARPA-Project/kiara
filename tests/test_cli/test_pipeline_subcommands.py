# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# def test_pipeline_subcommand():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "pipeline")
#     assert result.exit_code == 0
#     assert "Pipeline-related sub-commands" in result.stdout
#
#
# def test_pipeline_explain_subcommand():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "pipeline explain logic.xor")
#
#     assert result.exit_code == 0
#     assert "Pipeline structure" in result.stdout
#     assert "Processing stage: 2" in result.stdout
#
#     pipeline_file = os.path.join(
#         os.path.dirname(__file__),
#         "..",
#         "resources",
#         "pipelines",
#         "logic",
#         "logic_3.json",
#     )
#     abs_path = os.path.abspath(pipeline_file)
#     result = runner.invoke(cli, f"pipeline explain '{abs_path}'")
#
#     assert result.exit_code == 0
#     assert "Pipeline structure" in result.stdout
#     assert "Processing stage: 2" in result.stdout
#
#
# def test_pipeline_explain_steps_subcommand():
#
#     runner = CliRunner()
#     result = runner.invoke(cli, "pipeline explain-steps logic.xor")
#
#     assert result.exit_code == 0
#     assert "logic.or" in result.stdout
#     assert "logic.nand" in result.stdout
#     assert "logic.and" in result.stdout
#
#     pipeline_file = os.path.join(
#         os.path.dirname(__file__),
#         "..",
#         "resources",
#         "pipelines",
#         "logic",
#         "logic_3.json",
#     )
#     abs_path = os.path.abspath(pipeline_file)
#     result = runner.invoke(cli, f"pipeline explain '{abs_path}'")
#
#     assert result.exit_code == 0
#     assert "logic.and" in result.stdout
