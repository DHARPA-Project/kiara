# -*- coding: utf-8 -*-

#  Copyright (c) 2020-2021, Markus Binsteiner
#
#  Mozilla Public License Version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import copy
import os
import sys
from pathlib import Path
from subprocess import PIPE, Popen
from timeit import default_timer as timer
from typing import Dict, Mapping, Union

import orjson
from deepdiff import DeepHash

from kiara.defaults import kiara_app_dirs

KIARA_DOC_BUILD_CACHE_DIR = os.path.join(kiara_app_dirs.user_cache_dir, "doc_gen")
if not os.path.exists(KIARA_DOC_BUILD_CACHE_DIR):
    os.makedirs(KIARA_DOC_BUILD_CACHE_DIR)

os_env_vars = copy.deepcopy(os.environ)
os_env_vars["CONSOLE_WIDTH"] = "80"
os_env_vars["KAIRA_DATA_STORE"] = os.path.join(
    kiara_app_dirs.user_cache_dir, "data_store_1"
)


def log_msg(msg: str):
    print(msg)  # noqa


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
        max_height: Union[int, None] = None,
        cache_key: Union[str, None] = None,
        extra_env: Union[Dict[str, str], None] = None,
        fake_command: Union[str, None] = None,
        fail_ok: bool = False,
        repl_dict: Union[Mapping[str, str], None] = None,
    ):
        """Execute the provided command, save the output and return it to be used in documentation modules."""
        hashes = DeepHash(command)
        hash_str = hashes[command]
        hashes_env = DeepHash(extra_env)
        hashes_env_str = hashes_env[extra_env]

        hash_str = hash_str + "_" + hashes_env_str
        if cache_key:
            hash_str = hash_str + "_" + cache_key

        cache_file: Path = Path(os.path.join(KIARA_DOC_BUILD_CACHE_DIR, str(hash_str)))
        failed_cache_file: Path = Path(
            os.path.join(KIARA_DOC_BUILD_CACHE_DIR, f"{hash_str}.failed")
        )
        cache_info_file: Path = Path(
            os.path.join(KIARA_DOC_BUILD_CACHE_DIR), f"{hash_str}.command"
        )

        _run_env = dict(os_env_vars)
        if extra_env:
            _run_env.update(extra_env)

        if cache_file.is_file():
            stdout_str = cache_file.read_text()
            if repl_dict:
                for k, v in repl_dict.items():
                    stdout_str = stdout_str.replace(k, v)
        else:
            start = timer()

            cache_info = {
                "command": command,
                "extra_env": extra_env,
                "cmd_hash": hash_str,
                "cache_key": cache_key,
                "fail_ok": fail_ok,
                "started": start,
                "repl_dict": repl_dict,
            }

            log_msg(f"RUNNING: {' '.join(command)}")

            p = Popen(command, stdout=PIPE, stderr=PIPE, env=_run_env)
            stdout, stderr = p.communicate()

            stdout_str = stdout.decode("utf-8")
            stderr_str = stderr.decode("utf-8")

            if repl_dict:
                for k, v in repl_dict.items():
                    stdout_str = stdout_str.replace(k, v)
                    stderr_str = stderr_str.replace(k, v)

            log_msg("stdout:")
            log_msg(stdout_str)
            log_msg("stderr:")
            log_msg(stderr_str)

            cache_info["exit_code"] = p.returncode

            end = timer()
            if p.returncode == 0:

                # result = subprocess.check_output(command, env=_run_env)

                # stdout = result.decode()
                cache_file.write_bytes(stdout)
                cache_info["size"] = len(stdout)
                cache_info["duration"] = end - start
                cache_info["success"] = True
                cache_info["output_file"] = cache_file.as_posix()
                cache_info_file.write_bytes(orjson.dumps(cache_info))

                if failed_cache_file.exists():
                    failed_cache_file.unlink()
            else:

                cache_info["duration"] = end - start

                if fail_ok:
                    cache_info["size"] = len(stdout)
                    cache_info["success"] = True
                    cache_file.write_bytes(stdout)
                    cache_info["output_file"] = cache_file.as_posix()
                    cache_info_file.write_bytes(orjson.dumps(cache_info))
                    if failed_cache_file.exists():
                        failed_cache_file.unlink()
                else:
                    cache_info["size"] = len(stdout)
                    cache_info["success"] = False
                    failed_cache_file.write_bytes(stdout)
                    cache_info["output_file"] = failed_cache_file.as_posix()
                    cache_info_file.write_bytes(orjson.dumps(cache_info))
                    # stdout = f"Error: {e}\n\nStdout: {e.stdout}\n\nStderr: {e.stderr}"
                    # cache_info["size"] = len(stdout)
                    # cache_info["success"] = False
                    # print("stdout:")
                    # print(e.stdout)
                    # print("stderr:")
                    # print(e.stderr)
                    # failed_cache_file.write_text(stdout)
                    # cache_info["output_file"] = failed_cache_file.as_posix()
                    # cache_info_file.write_bytes(orjson.dumps(cache_info))
                    if os.getenv("FAIL_DOC_BUILD_ON_ERROR") == "true":
                        sys.exit(1)

        if fake_command:
            command_str = fake_command
        else:
            command_str = " ".join(command)

        if split_command_and_output and print_command:
            _c = f"\n```bash\n{command_str}\n```\n"
            _output = "```\n" + stdout_str + "\n```\n"
            if max_height is not None and max_height > 0:
                _output = f"<div style='max-height:{max_height}px;overflow:auto'>\n{_output}\n</div>"
            _stdout = _c + _output
        else:
            if print_command:
                _stdout = f"> {command_str}\n{stdout_str}"
            if code_block:
                _stdout = "```\n" + _stdout + "\n```\n"

            if max_height is not None and max_height > 0:
                _stdout = f"<div style='max-height:{max_height}px;overflow:auto'>\n{_stdout}\n</div>"

        return _stdout

    @env.macro
    def inline_file_as_codeblock(path, format: str = ""):
        """Import external file and return its content as a markdown code block."""
        f = Path(path)
        return f"```{format}\n{f.read_text()}\n```"
