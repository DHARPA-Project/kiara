# -*- coding: utf-8 -*-
import types
from inspect import getmembers, isfunction
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Union

from kiara.interfaces.python_api import JobDesc
from kiara.utils import log_exception
from kiara.utils.files import get_data_from_file


def list_job_descs(jobs_folder: Path):
    for f in jobs_folder.glob("*"):
        try:
            job_desc = JobDesc.create_from_file(f)
            yield job_desc
        except Exception as e:
            log_exception(e)


def get_tests_for_job(
    job_alias: str, job_tests_folder: Path
) -> Union[None, Mapping[str, Union[Any, Callable]]]:
    """Get tests for a job.

    In case the tests are Python code, this will use 'exec' to execute it, which is
    usually discouraged. However, this is only used for testing, and it makes it easier
    to create those tests, which is why it is used here.
    """

    module_base_name = f"kiara_tests.job_tests.{job_alias}"

    test_folder = job_tests_folder / job_alias
    tests = {}
    if test_folder.is_dir():
        for f in test_folder.glob("*.py"):
            test_name = f.stem
            code = f.read_text()
            module_name = f"{module_base_name}.{test_name}"
            module = types.ModuleType(module_name)
            exec(code, module.__dict__)  # noqa

            for func in getmembers(module, isfunction):
                tests[func[0]] = func[1]

    test_checks = test_folder / "outputs.json"
    if not test_checks.is_file():
        test_checks = test_folder / "outputs.yaml"

    if test_checks.is_file():
        test_data: Dict[str, Any] = get_data_from_file(test_checks)
    else:
        test_data = {}

    for k, v in test_data.items():
        if k in tests.keys():
            raise Exception(f"Duplicate test name: {k}")
        tests[k] = v

    return tests
