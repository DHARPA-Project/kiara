# -*- coding: utf-8 -*-

#  Copyright (c) 2020-2021, Markus Binsteiner
#
#  Mozilla Public License Version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import copy
import os
import subprocess
import sys
from deepdiff import DeepHash
from pathlib import Path
from typing import Dict, Optional

from kiara.defaults import kiara_app_dirs

CACHE_DIR = os.path.join(kiara_app_dirs.user_cache_dir, "doc_gen")
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

os_env_vars = copy.copy(os.environ)
os_env_vars["CONSOLE_WIDTH"] = "80"
os_env_vars["KAIRA_DATA_STORE"] = os.path.join(
    kiara_app_dirs.user_cache_dir, "data_store_1"
)


def define_env(env):
    """
    Helper macros for Python project documentation.

    Currently, those macros are available (check the source code for more details):

    ## ``cli``

    Execute a command on the command-line, capture the output and return it to be used in a documentation page.

    ## ``inline_file_as_codeblock``

    Read an external file, and return its content as a markdown code block.
    """

    # env.variables["baz"] = "John Doe"

    @env.macro
    def cli(
        *command,
        print_command: bool = True,
        code_block: bool = True,
        split_command_and_output: bool = True,
        max_height: Optional[int] = None,
        cache_key: Optional[str] = None,
        extra_env: Optional[Dict[str, str]] = None,
        fake_command: Optional[str] = None,
    ):
        """Execute the provided command, save the output and return it to be used in documentation modules."""

        hashes = DeepHash(command)
        hash_str = hashes[command]
        hashes_env = DeepHash(extra_env)
        hashes_env_str = hashes_env[extra_env]

        hash_str = hash_str + "_" + hashes_env_str
        if cache_key:
            hash_str = hash_str + "_" + cache_key

        cache_file: Path = Path(os.path.join(CACHE_DIR, str(hash_str)))

        _run_env = dict(os_env_vars)
        if extra_env:
            _run_env.update(extra_env)

        if cache_file.is_file():
            stdout = cache_file.read_text()
        else:
            try:
                print(f"RUNNING: {' '.join(command)}")
                result = subprocess.check_output(command, env=_run_env)
                stdout = result.decode()
                cache_file.write_text(stdout)
            except subprocess.CalledProcessError as e:
                stdout = f"Error: {e}\n\nStdout: {e.stdout}\n\nStderr: {e.stderr}"
                print("stdout:")
                print(e.stdout)
                print("stderr:")
                print(e.stderr)
                if os.getenv("FAIL_DOC_BUILD_ON_ERROR") == "true":
                    sys.exit(1)

        if fake_command:
            command_str = fake_command
        else:
            command_str = " ".join(command)
        if split_command_and_output and print_command:
            _c = f"\n``` console\n{command_str}\n```\n"
            _output = "``` console\n" + stdout + "\n```\n"
            if max_height is not None and max_height > 0:
                _output = f"<div style='max-height:{max_height}px;overflow:auto'>\n{_output}\n</div>"
            _stdout = _c + _output
        else:
            if print_command:
                _stdout = f"> {command_str}\n{stdout}"
            if code_block:
                _stdout = "``` console\n" + _stdout + "\n```\n"

            if max_height is not None and max_height > 0:
                _stdout = f"<div style='max-height:{max_height}px;overflow:auto'>\n{_stdout}\n</div>"

        return _stdout

    @env.macro
    def inline_file_as_codeblock(path, format: str = ""):
        """Import external file and return its content as a markdown code block."""

        f = Path(path)
        return f"```{format}\n{f.read_text()}\n```"
