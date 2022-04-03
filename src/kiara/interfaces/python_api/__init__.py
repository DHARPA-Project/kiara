import os
import uuid
from typing import Optional, Mapping, Any, Dict, Union, List

from pydantic import BaseModel, Field
import structlog
from rich import box
from rich.console import RenderableType, RenderGroup
from rich.table import Table

from kiara import Kiara
from kiara.exceptions import NoSuchExecutionTargetException, FailedJobException
from kiara.models.module.jobs import JobConfig, JobStatus
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import Operation
from kiara.models.values.value import Value, ValueSet
from kiara.utils import is_debug

logger = structlog.getLogger()

class StoreValueResult(BaseModel):

    value: Value = Field(description="The stored value.")
    aliases: List[str] = Field(description="The aliases that where assigned to the value when stored.")
    error: Optional[str] = Field(description="An error that occured while trying to store.")

class StoreValuesResult(BaseModel):

    __root__: Dict[str, StoreValueResult]

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=True, show_lines=False, box=box.SIMPLE)
        table.add_column("field", style="b")
        table.add_column("data type", style="i")
        table.add_column("stored id", style="i")
        table.add_column("alias(es)")

        for field_name, value_result in self.__root__.items():
            row = [field_name, str(value_result.value.value_schema.type), str(value_result.value.value_id)]
            if value_result.aliases:
                row.append(', '.join(value_result.aliases))
            else:
                row.append("")
            table.add_row(*row)

        return table

class KiaraOperation(object):

    def __init__(self, kiara: "Kiara", operation_name: str, operation_config: Optional[Mapping[str, Any]]=None):

        self._kiara: Kiara = kiara
        self._operation_name: str = operation_name
        if operation_config is None:
            operation_config = {}
        else:
            operation_config = dict(operation_config)
        self._operation_config: Dict[str, Any] = operation_config

        self._inputs_raw: Dict[str, Any] = {}

        self._operation: Optional[Operation] = None
        self._inputs: Optional[ValueSet] = None

        self._job_config: Optional[JobConfig] = None

        self._queued_jobs: Dict[uuid.UUID, Dict[str, Any]] = {}
        self._last_job: Optional[uuid.UUID] = None
        self._results: Dict[uuid.UUID, ValueSet] = {}

    def validate(self):

        self.job_config  # noqa

    def _invalidate(self):

        self._job_config = None

    @property
    def operation_inputs(self) -> ValueSet:

        if self._inputs is not None:
            return self._inputs

        self._invalidate()
        self._inputs = self._kiara.data_registry.create_valueset(self._inputs_raw, self.operation.inputs_schema)
        return self._inputs

    def set_input(self, field: Optional[str], value: Any=None):

        if field is None:
            if value is None:
                self._inputs_raw.clear()
                self._invalidate()
                return
            else:
                if not isinstance(value, Mapping):
                    raise Exception(f"Can't set inputs dictionary (if no key is provided, value must be 'None' or of type 'Mapping').")

                self._inputs_raw.clear()
                self.set_inputs(**value)
                self._invalidate()
                return
        else:
            old = self._inputs_raw.get(field, None)
            self._inputs_raw[field] = value
            if old != value:
                self._invalidate()
            return

    def set_inputs(self, **inputs: Any):

        changed = False
        for k, v in inputs.items():
            old = self._inputs_raw.get(k, None)
            self._inputs_raw[k] = v
            if old != v:
                changed = True

        if changed:
            self._invalidate()

        return

    @property
    def operation_name(self) -> str:
        return self._operation_name

    @operation_name.setter
    def operation_name(self, operation_name: str):
        self._operation_name = operation_name
        self._operation = None

    @property
    def operation_config(self) -> Mapping[str, Any]:
        return self._operation_config

    def set_operation_config_value(self, key: Optional[str], value: Any=None) -> Mapping[str, Any]:

        if key is None:
            if value is None:
                old = bool(self._operation_config)
                self._operation_config.clear()
                if old:
                    self._operation = None
                return self._operation_config
            else:
                try:
                    old = self._operation_config
                    self._operation_config = dict(value)
                    if old != self._operation_config:
                        self._operation = None
                    return self._operation_config
                except Exception as e:
                    raise Exception(f"Can't set configuration value dictionary (if no key is provided, value must be 'None' or of type 'Mapping'): {e}")

        self._operation_config[key] = value
        self._invalidate()
        return self._operation_config

    @property
    def operation(self) -> "Operation":

        if self._operation is not None:
            return self._operation

        self._invalidate()

        module_or_operation = self._operation_name
        operation: Optional[Operation] = None
        if isinstance(module_or_operation, str):
            if module_or_operation in self._kiara.operation_registry.operation_ids:

                operation = self._kiara.operation_registry.get_operation(module_or_operation)
                if self._operation_config:
                    raise Exception(
                        f"Specified run target '{module_or_operation}' is an operation, additional module configuration is not allowed."
                    )

        elif module_or_operation in self._kiara.module_type_names:

            manifest = Manifest(
                module_type=module_or_operation, module_config=self._operation_config
            )

            module = self._kiara.create_module(manifest=manifest)
            operation = Operation.create_from_module(module)

        elif os.path.isfile(module_or_operation):
            raise NotImplementedError()
            # module_name = kiara_obj.register_pipeline_description(
            #     module_or_operation, raise_exception=True
            # )

        if operation is None:
            merged = set(self._kiara.module_type_names)
            merged.update(self._kiara.operation_registry.operation_ids)
            raise NoSuchExecutionTargetException(selected_target=self.operation_name,
                msg=f"Invalid run target name '{module_or_operation}'. Must be a path to a pipeline file, or one of the available modules/operations.",
                available_targets=sorted(merged)
            )

        self._operation = operation
        return self._operation

    @property
    def job_config(self) -> JobConfig:

        if self._job_config is not None:
            return self._job_config

        self._job_config = self.operation.prepare_job_config(kiara=self._kiara, inputs=self.operation_inputs)
        return self._job_config

    def queue_job(self) -> uuid.UUID:

        job_config = self.job_config
        operation = self.operation
        inputs = self.operation_inputs

        job_id = self._kiara.job_registry.execute_job(job_config=job_config, wait=False)

        self._queued_jobs[job_id] = {"job_config": job_config, "operation": operation, "inputs": inputs}
        self._last_job = job_id
        return job_id

    def retrieve_result(self, job_id: Optional[uuid.UUID]=None) -> ValueSet:

        if job_id in self._results.keys():
            return self._results[job_id]

        if job_id is None:
            job_id = self._last_job

        if job_id is None:
            raise Exception("No job queued (yet).")

        operation: Operation = self._queued_jobs[job_id]["operation"]  # type: ignore

        status = self._kiara.job_registry.get_job_status(job_id=job_id)

        if status == JobStatus.FAILED:
            job = self._kiara.job_registry.get_active_job(job_id=job_id)
            raise FailedJobException(job=job)

        outputs = self._kiara.job_registry.retrieve_result(job_id)
        outputs = operation.process_job_outputs(outputs=outputs)
        self._results[job_id] = outputs
        return outputs

    def save_result(self, job_id: Optional[uuid.UUID]=None, aliases: Union[None, str, Mapping]=None) -> StoreValuesResult:

        result = self.retrieve_result(job_id=job_id)

        if aliases is None:
            alias_map: Dict[str, List[str]] = {}
        elif isinstance(aliases, str):
            alias_map = {}
            for field_name in result.field_names:
                alias_map[field_name] = [f"{aliases}-{field_name}"]
        elif isinstance(aliases, Mapping):
            alias_map = {}
            for field_name in aliases.keys():
                if field_name in result.field_names:
                    if isinstance(aliases[field_name], str):
                        alias_map[field_name] = [aliases[field_name]]
                    else:
                        alias_map[field_name] = sorted(aliases[field_name])
                else:
                    logger.warning("ignore.field_alias", ignored_field_name=field_name, reason="field name not in results", available_field_names=sorted(result.field_names))
                    continue
        else:
            raise Exception(f"Invalid type '{type(aliases)}' for aliases parameter, must be string or mapping.")

        stored = {}
        for field_name, field_aliases in alias_map.items():

            value = result.get_value_obj(field_name)
            try:
                self._kiara.data_registry.store_value(value=value, skip_if_exists=True)

                msg = f"   -> done, id: [i]{value.value_id}[/i]"
                if field_aliases:
                    msg = msg + f", aliases: [i]{', '.join(field_aliases)}[/i]"

                stored[field_name] = StoreValueResult.construct(value=value, aliases=field_aliases, error=None)

            except Exception as e:
                if is_debug():
                    import traceback
                    traceback.print_exc()
                stored[field_name] = StoreValueResult.construct(value-value, aliases=field_aliases, error=str(e))

        self._kiara.job_registry.store_job_record(job_id=job_id)

        return StoreValuesResult.construct(__root__=stored)

