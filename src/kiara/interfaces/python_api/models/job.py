# -*- coding: utf-8 -*-
import os.path
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Union

from dag_cbor import IPLDKind
from pydantic import BaseModel, Field, root_validator, validator

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.utils.cli import terminal_print
from kiara.utils.files import get_data_from_file
from kiara.utils.string_vars import replace_var_names_in_obj

if TYPE_CHECKING:
    from kiara.interfaces.python_api import KiaraAPI
    from kiara.models.module.operation import Operation
    from kiara.models.values.value import ValueMap


class JobDesc(KiaraModel):
    """An object describing a compute job with both raw or referenced inputs."""

    _kiara_model_id = "instance.job_desc"

    @classmethod
    def create_from_file(cls, path: Union[str, Path]):
        run_data = cls.parse_from_file(path)
        return cls(**run_data)

    @classmethod
    def parse_from_file(cls, path: Union[str, Path]):

        if isinstance(path, str):
            path = Path(path)

        if not path.is_file():
            raise KiaraException(
                f"Can't load job description, invalid file path: '{path}'"
            )

        data = get_data_from_file(path)

        repl_dict: Dict[str, Any] = {"this_dir": path.parent.absolute().as_posix()}

        try:
            run_data = cls.parse_data(
                data=data, var_repl_dict=repl_dict, alias=path.stem
            )
            return run_data
        except Exception as e:
            raise KiaraException(f"Invalid run description in file '{path}': {e}")

    @classmethod
    def parse_data(
        cls,
        data: Mapping[str, Any],
        var_repl_dict: Union[Mapping[str, Any], None] = None,
        alias: Union[str, None] = None,
    ):

        if not isinstance(data, Mapping):
            raise KiaraException("Job description data is not a mapping.")

        if "operation" not in data.keys():
            raise KiaraException("Missing 'operation' key")

        if var_repl_dict:
            run_data = replace_var_names_in_obj(data, repl_dict=var_repl_dict)
        else:
            run_data = data

        if alias:
            run_data["job_alias"] = alias

        return run_data

    @classmethod
    def create_from_data(
        cls,
        data: Mapping[str, Any],
        var_repl_dict: Union[Mapping[str, Any], None] = None,
        alias: Union[str, None] = None,
    ) -> "JobDesc":

        run_data = cls.parse_data(data=data, var_repl_dict=var_repl_dict, alias=alias)
        return cls(**run_data)

    job_alias: str = Field(description="The alias for the job.", default="default")
    operation: str = Field(description="The operation id or module type.")
    module_config: Union[Mapping[str, Any], None] = Field(
        default=None, description="The configuration for the module."
    )
    inputs: Dict[str, Any] = Field(
        description="The inputs for the job.", default_factory=dict
    )
    doc: DocumentationMetadataModel = Field(
        description="A description/doc for this job.",
        default_factory=DocumentationMetadataModel.create,
    )
    save: Dict[str, str] = Field(
        description="Configuration on how/whether to save the job results.",
        default_factory=dict,
    )

    def _retrieve_data_to_hash(self) -> IPLDKind:
        def get_hash(v: Any):
            if hasattr(v, "instance_cid"):
                return v.instance_cid
            elif hasattr(v, "value_id"):
                return str(v.value_id)
            elif isinstance(v, uuid.UUID):
                return str(v)
            elif isinstance(v, Mapping):
                return {get_hash(k): get_hash(v) for k, v in v.items()}
            return v

        inputs_hash = {k: get_hash(v) for k, v in self.inputs.items()}
        return {
            "operation": self.operation,
            "module_config": self.module_config,  # type: ignore
            "inputs": inputs_hash,
            "save": self.save,  # type: ignore
        }

    @root_validator(pre=True)
    def validate_inputs(cls, values):

        if len(values) == 1 and "data" in values.keys():
            data = values["data"]
            if isinstance(data, str):
                if os.path.isfile(data):
                    data = Path(data)

            if isinstance(data, Path):
                run_data = cls.parse_from_file(data)
                return run_data
            else:
                values = data
        return values

    @validator("doc", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

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


class RunSpec(BaseModel):
    """A list of jobs, ran one after the other, incl saving of results."""

    @classmethod
    def create_from_file(cls, path: Union[str, Path]):

        if isinstance(path, str):
            path = Path(path)

        if not path.is_file():
            raise KiaraException(f"Can't load run spec, invalid file path: '{path}'")

        data = get_data_from_file(path)

        repl_dict: Dict[str, Any] = {"this_dir": path.parent.absolute().as_posix()}

        try:
            run = cls.create_from_data(
                data=data, var_repl_dict=repl_dict, alias=path.stem
            )
            return run
        except Exception as e:
            raise KiaraException(f"Invalid run description in file '{path}': {e}")

    @classmethod
    def create_from_data(
        cls,
        data: Mapping[str, Any],
        var_repl_dict: Union[Mapping[str, Any], None] = None,
        alias: Union[str, None] = None,
    ):

        if not isinstance(data, Mapping):
            raise KiaraException("Run spec data is not a mapping.")

        if "jobs" not in data.keys():
            raise KiaraException("Missing 'jobs' key")

        if var_repl_dict:
            run_data = replace_var_names_in_obj(data, repl_dict=var_repl_dict)
        else:
            run_data = data

        if alias:
            run_data["run_alias"] = alias

        instance = cls(**run_data)
        return instance

    run_alias: str = Field(description="The alias for the run.")
    jobs: List[JobDesc] = Field(description="The jobs to run.", default_factory=list)
    doc: DocumentationMetadataModel = Field(
        description="A description/doc for this run.",
        default_factory=DocumentationMetadataModel.create,
    )

    @root_validator(pre=True)
    def validate_inputs(cls, values):
        if "jobs" not in values.keys():
            raise ValueError("Missing required 'jobs' key.")

        jobs = values["jobs"]
        if not isinstance(jobs, list):
            raise ValueError("Invalid 'jobs' value, must be a list.")

        new_jobs = []
        for job in jobs:
            if isinstance(job, JobDesc):
                job_spec = job
            elif isinstance(job, Mapping):
                job_spec = JobDesc(**job)
            elif isinstance(job, (str, Path)):
                job_spec = JobDesc.create_from_file(job)
            else:
                raise ValueError(f"Invalid job spec type: {job}")

            # TODO: validate 'save' fields
            new_jobs.append(job_spec)

        values["jobs"] = new_jobs
        return values

    @validator("doc", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)


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

        print(f"Running tests for job '{self._job_desc.job_alias}'...")  # noqa

        result = self.run_job()
        self.check_result(result)

    def run_job(self) -> "ValueMap":

        print(f"Running checks for job '{self._job_desc.job_alias}'...")  # noqa
        try:
            result = self._kiara_api.run_job(operation=self._job_desc)
        except Exception as e:
            exc = KiaraException(f"Failed to run job '{self._job_desc.job_alias}': {e}")
            terminal_print(exc)
            raise e
        return result

    def check_result(self, result: "ValueMap"):

        try:

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
        except Exception as e:
            exc = KiaraException(
                f"Failed to run test '{test}' for job '{self._job_desc.job_alias}': {e}"
            )
            terminal_print(exc)
            raise e
        return result
