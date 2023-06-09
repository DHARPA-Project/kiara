# -*- coding: utf-8 -*-
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Union

from pydantic import BaseModel, Field

from kiara.exceptions import KiaraException
from kiara.utils.files import get_data_from_file
from kiara.utils.string_vars import replace_var_names_in_obj

if TYPE_CHECKING:
    from kiara.interfaces.python_api import KiaraAPI
    from kiara.models.module.operation import Operation
    from kiara.models.values.value import ValueMap


class JobDesc(BaseModel):
    """An object describing a compute job with both raw or referenced inputs."""

    @classmethod
    def create_from_file(cls, path: Union[str, Path]):

        if isinstance(path, str):
            path = Path(path)

        if not path.is_file():
            raise KiaraException(
                f"Can't load job description, invalid file path: '{path}'"
            )

        data = get_data_from_file(path)

        if not isinstance(data, Mapping):
            raise KiaraException(f"Invalid job description in file: '{path}'")

        if "operation" not in data.keys():
            raise KiaraException(
                f"Invalid job description in file: '{path}', missing 'operation' key"
            )

        repl_dict: Dict[str, Any] = {"this_dir": path.parent.as_posix()}
        job_data = replace_var_names_in_obj(data, repl_dict=repl_dict)
        job_data["job_alias"] = path.stem
        return cls(**job_data)

    job_alias: str = Field(description="The alias for the job.")
    operation: str = Field(description="The operation id or module type.")
    module_config: Union[Mapping[str, Any], None] = Field(
        default=None, description="The configuration for the module."
    )
    inputs: Dict[str, Any] = Field(
        description="The inputs for the job.", default_factory=dict
    )

    def get_operation(self, kiara_api: "KiaraAPI") -> "Operation":

        if not self.module_config:
            operation = kiara_api.get_operation(self.operation, allow_external=True)
        else:
            data = {
                "module_type": self.operation,
                "module_config": self.module_config,
            }
            operation = kiara_api.get_operation(operation=data, allow_external=False)

        return operation


class JobTest(object):
    def __init__(
        self,
        kiara_api: "KiaraAPI",
        job_desc: JobDesc,
        tests: Union[Mapping[str, Mapping[str, Any]], None] = None,
    ):

        self._kiara_api: KiaraAPI = kiara_api
        self._job_desc = job_desc
        if tests is None:
            tests = {}
        self._tests = tests

    def run_tests(self):

        result = self.run_job()
        self.check_result(result)

    def run_job(self) -> "ValueMap":

        result = self._kiara_api.run_job(operation=self._job_desc)
        return result

    def check_result(self, result: "ValueMap"):

        import inspect

        from kiara.api import Value

        for test_name, test in self._tests.items():

            if not callable(test):
                tokens = test_name.split("::")
                value = result.get_value_obj(tokens[0])

                if len(tokens) > 1:
                    data_to_test = self._kiara_api.query_value(
                        value, "::".join(tokens[1:])
                    )
                else:
                    data_to_test = value

                if isinstance(data_to_test, Value):
                    data_to_test = data_to_test.data

                if test != data_to_test:
                    raise AssertionError(
                        f"Test pattern '{test_name}' for job '{self._job_desc.job_alias}' failed: {data_to_test} (result) != {test} (expected)"
                    )

            else:

                args = inspect.signature(test)
                arg_values: List[Any] = []

                for arg_name in args.parameters.keys():
                    if arg_name == "kiara_api":
                        arg_values.append(self._kiara_api)
                    elif arg_name == "outputs":
                        arg_values.append(result)
                    elif arg_name in result.field_names:
                        arg_values.append(result.get_value_obj(arg_name))
                    else:
                        raise KiaraException(
                            f"Invalid test function: '{test_name}', argument '{arg_name}' not available in result. Available arguments: {result.field_names} or 'outputs' for all outputs."
                        )

                test(*arg_values)
