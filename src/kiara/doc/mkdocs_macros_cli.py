# -*- coding: utf-8 -*-

#  Copyright (c) 2020, Markus Binsteiner
#
#  Mozilla Public License Version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import copy
import os
import subprocess
import sys
from deepdiff import DeepHash
from pathlib import Path
from typing import Optional

from kiara.defaults import kiara_app_dirs

CACHE_DIR = os.path.join(kiara_app_dirs.user_cache_dir, "doc_gen")
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

os_env_vars = copy.copy(os.environ)
os_env_vars["CONSOLE_WIDTH"] = "80"


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
        max_height: Optional[int] = None,
    ):
        """Execute the provided command, save the output and return it to be used in documentation pages."""

        hashes = DeepHash(command)
        hash_str = hashes[command]

        cache_file: Path = Path(os.path.join(CACHE_DIR, str(hash_str)))
        if cache_file.is_file():
            stdout = cache_file.read_text()
        else:
            try:
                print(f"RUNNING: {' '.join(command)}")
                result = subprocess.check_output(command, env=os_env_vars)
                stdout = result.decode()
                cache_file.write_text(stdout)
            except subprocess.CalledProcessError as e:
                print("stdout:")
                print(e.stdout)
                print("stderr:")
                print(e.stderr)
                if os.getenv("FAIL_DOC_BUILD_ON_ERROR") == "true":
                    sys.exit(1)

        if print_command:
            stdout = f"> {' '.join(command)}\n{stdout}"
        if code_block:
            stdout = "``` console\n" + stdout + "\n```\n"

        if max_height is not None and max_height > 0:
            stdout = f"<div style='max-height:{max_height}px;overflow:auto'>\n{stdout}\n</div>"

        return stdout

    @env.macro
    def inline_file_as_codeblock(path, format: str = ""):
        """Import external file and return its content as a markdown code block."""

        f = Path(path)
        return f"```{format}\n{f.read_text()}\n```"
