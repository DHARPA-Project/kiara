# -*- coding: utf-8 -*-
import structlog
import uuid
from slugify import slugify
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Tuple, Union

from kiara.defaults import NONE_VALUE_ID, NOT_SET_VALUE_ID
from kiara.interfaces.python_api.operation import KiaraOperation
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.events.pipeline import ChangedValue, PipelineEvent
from kiara.models.module.jobs import ExecutionContext, JobConfig
from kiara.models.module.pipeline import (
    PipelineConfig,
    PipelineStep,
    StepStatus,
    StepValueAddress,
    create_input_alias_map,
    create_output_alias_map,
)
from kiara.models.module.pipeline.controller import SinglePipelineController
from kiara.models.module.pipeline.pipeline import Pipeline, PipelineInfo
from kiara.models.python_class import KiaraModuleInstance
from kiara.models.values.value import ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.models.workflow import WorkflowDetails, WorkflowInfo, WorkflowState
from kiara.utils import find_free_id, log_exception

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()


class WorkflowPipelineController(SinglePipelineController):
    """A [PipelineController][kiara.models.modules.pipeline.controller.PipelineController] that executes all pipeline steps non-interactively.

    This is the default implementation of a ``PipelineController``, and probably the most simple implementation of one.
    It waits until all inputs are set, after which it executes all pipeline steps in the required order.

    Arguments:
        pipeline: the pipeline to control
        auto_process: whether to automatically start processing the pipeline as soon as the input set is valid
    """

    def __init__(
        self,
        kiara: "Kiara",
    ):

        self._is_running: bool = False
        super().__init__(job_registry=kiara.job_registry)

    def _pipeline_event_occurred(self, event: PipelineEvent):

        if event.pipeline_id != self.pipeline.pipeline_id:
            return

        self._pipeline_details = None

    def process_pipeline(self) -> Mapping[uuid.UUID, uuid.UUID]:

        log = logger.bind(pipeline_id=self.pipeline.pipeline_id)
        if self._is_running:
            log.debug(
                "ignore.pipeline_process",
                reason="Pipeline already running.",
            )
            raise Exception("Pipeline already running.")

        log.debug("execute.pipeline")
        self._is_running = True

        result: Dict[uuid.UUID, uuid.UUID] = {}
        try:
            for idx, stage in enumerate(
                self.pipeline.structure.processing_stages, start=1
            ):

                log.debug(
                    "execute.pipeline.stage",
                    stage=idx,
                )

                job_ids = {}
                stage_failed = False
                for step_id in stage:

                    log.debug(
                        "execute.pipeline.step",
                        step_id=step_id,
                    )

                    try:
                        job_id = self.process_step(step_id)
                        job_ids[step_id] = job_id
                    except Exception as e:
                        # TODO: cancel running jobs?
                        log_exception(e)
                        log.error(
                            "error.processing.pipeline",
                            step_id=step_id,
                            error=e,
                        )
                        stage_failed = True

                output_job_map = self.set_processing_results(job_ids=job_ids)
                result.update(output_job_map)
                if not stage_failed:
                    log.debug(
                        "execute_finished.pipeline.stage",
                        stage=idx,
                    )
                else:
                    log.debug(
                        "execute_failed.pipeline.stage",
                        stage=idx,
                    )
                    break
        except Exception as e:
            log_exception(e)
        finally:
            self._is_running = False

        log.debug("execute_finished.pipeline")
        return result


class WorkflowStatus(KiaraModel):
    pass


class Workflow(object):
    @classmethod
    def load(cls, workflow: Union[uuid.UUID, str]):

        pass

    @classmethod
    def create(
        cls,
        alias: Union[None, str] = None,
        blueprint: Union[str, None] = None,
        doc: Union[None, str, DocumentationMetadataModel] = None,
        kiara: Union[None, "Kiara"] = None,
        overwrite_existing: bool = False,
    ):

        if kiara is None:
            from kiara.context import Kiara

            kiara = Kiara.instance()

        operation: Union[None, KiaraOperation] = None
        if blueprint:
            operation = KiaraOperation(kiara=kiara, operation_name=blueprint)
            if doc is None:
                doc = operation.operation.doc

        details = WorkflowDetails(documentation=doc)

        workflow_obj = Workflow(kiara=kiara, workflow=details)
        if alias:
            workflow_obj.workflow_alias = alias
        if blueprint:
            assert operation

            module = operation.operation.module
            if isinstance(module.config, PipelineConfig):
                config: PipelineConfig = module.config
            else:
                raise NotImplementedError()

            workflow_obj.add_steps(*config.steps)

        return workflow_obj

    def __init__(
        self, kiara: "Kiara", workflow: Union[uuid.UUID, WorkflowDetails, str, None]
    ):

        self._kiara: Kiara = kiara

        self._execution_context: ExecutionContext = ExecutionContext()
        self._pipeline_controller: WorkflowPipelineController = (
            WorkflowPipelineController(kiara=self._kiara)
        )

        workflow_id = None
        self._workflow_alias: Union[None, str] = None
        self._is_stored: bool = False
        self._workflow_details: WorkflowDetails = None  # type: ignore

        if workflow is None:
            workflow_id = None
        if isinstance(workflow, WorkflowDetails):
            workflow_id = workflow.workflow_id
        elif isinstance(workflow, uuid.UUID):
            workflow_id = workflow
        elif isinstance(workflow, str):
            try:
                workflow_id = uuid.UUID(workflow)
            except Exception:
                # means it is meant to be an alias
                self._workflow_alias = workflow
                try:
                    self._workflow_details = (
                        self._kiara.workflow_registry.get_workflow_details(workflow)
                    )
                    self._is_stored = True
                    workflow_id = self._workflow_details.workflow_id
                except Exception:
                    pass

        if workflow_id:
            if self._workflow_details is None:
                try:
                    self._workflow_details = (
                        self._kiara.workflow_registry.get_workflow_details(
                            workflow=workflow_id
                        )
                    )
                    self._is_stored = True
                except Exception:
                    pass

        if self._workflow_details is None:
            self._workflow_details = WorkflowDetails()

        self._all_inputs: Dict[str, Any] = {}
        self._current_inputs: Union[Dict[str, uuid.UUID], None] = None
        self._current_outputs: Union[Dict[str, uuid.UUID], None] = None

        self._steps: Dict[str, PipelineStep] = {}

        self._pipeline_input_aliases: Dict[str, str] = {}
        self._pipeline_output_aliasess: Dict[str, str] = {}

        # self._current_state_cid: Union[None, CID] = None
        self._state_cache: Dict[str, WorkflowState] = {}

        self._job_id_cache: Dict[uuid.UUID, uuid.UUID] = {}
        """Cache to save job ids per output value(s), in order to save jobs if output values are saved."""

        self._pipeline: Union[Pipeline, None] = None
        self._pipeline_info: Union[PipelineInfo, None] = None
        self._current_info: Union[WorkflowInfo, None] = None
        self._current_state: Union[WorkflowState, None] = None

        if self._workflow_details.workflow_states:
            self.load_state()

    @property
    def workflow_id(self) -> uuid.UUID:
        return self._workflow_details.workflow_id

    @property
    def workflow_alias(self) -> Union[None, str]:
        return self._workflow_alias

    @workflow_alias.setter
    def workflow_alias(self, alias: str):
        self._workflow_alias = alias
        # TODO: register in registry

    @property
    def details(self) -> WorkflowDetails:
        return self._workflow_details

    @property
    def current_inputs_schema(self) -> Mapping[str, ValueSchema]:
        return self.pipeline.structure.pipeline_inputs_schema

    @property
    def current_input_names(self) -> List[str]:
        return sorted(self.current_inputs_schema.keys())

    @property
    def current_outputs_schema(self) -> Mapping[str, ValueSchema]:
        return self.pipeline.structure.pipeline_outputs_schema

    @property
    def current_output_names(self) -> List[str]:
        return sorted(self.current_outputs_schema.keys())

    @property
    def current_inputs(self) -> Mapping[str, uuid.UUID]:

        if self._current_inputs is None:
            self._apply_inputs()
        assert self._current_inputs is not None
        return self._current_inputs

    @property
    def current_outputs(self) -> Mapping[str, uuid.UUID]:

        if self._current_outputs is None:
            try:
                self.process_steps()
            except Exception:
                self._current_outputs = self.pipeline.get_current_pipeline_outputs()

        assert self._current_outputs is not None
        return self._current_outputs

    @property
    def current_output_values(self) -> ValueMap:
        return self._kiara.data_registry.load_values(values=self.current_outputs)

    @property
    def current_state(self) -> WorkflowState:

        if self._current_state is not None:
            return self._current_state

        self._current_state = WorkflowState.create_from_workflow(self)
        self._state_cache[self._current_state.instance_id] = self._current_state
        return self._current_state

    @property
    def pipeline(self) -> Pipeline:

        if self._pipeline is not None:
            return self._pipeline

        self._invalidate_pipeline()

        # if not self._steps:
        #     raise Exception("Can't assemble pipeline, no steps set (yet).")
        steps = list(self._steps.values())
        input_aliases_temp = create_input_alias_map(steps=steps)
        input_aliases = {}
        for k, v in input_aliases_temp.items():
            if k in self._pipeline_input_aliases.keys():
                input_aliases[k] = self._pipeline_input_aliases[k]
            else:
                input_aliases[k] = v

        if not self._pipeline_output_aliasess:
            output_aliases = create_output_alias_map(steps=steps)
        else:
            output_aliases = self._pipeline_output_aliasess

        pipeline_config = PipelineConfig.from_config(
            pipeline_name="__workflow__",
            data={
                "steps": steps,
                "doc": self.details.documentation,
                "input_aliases": input_aliases,
                "output_aliases": output_aliases,
            },
        )
        structure = pipeline_config.structure
        self._pipeline = Pipeline(structure=structure, kiara=self._kiara)
        self._pipeline_controller.pipeline = self._pipeline
        return self._pipeline

    def _apply_inputs(self) -> Mapping[str, Mapping[str, Mapping[str, ChangedValue]]]:

        pipeline = self.pipeline

        inputs_to_set = {}
        for field_name, value in self._all_inputs.items():
            if value in [NONE_VALUE_ID, NOT_SET_VALUE_ID]:
                continue
            if field_name in pipeline.structure.pipeline_inputs_schema.keys():
                inputs_to_set[field_name] = value

        logger.debug(
            "workflow.apply_inputs",
            workflow_id=str(self.workflow_id),
            keys=", ".join(inputs_to_set.keys()),
        )

        changed: Mapping[
            str, Mapping[str, Mapping[str, ChangedValue]]
        ] = pipeline.set_pipeline_inputs(inputs=inputs_to_set)

        self._current_inputs = pipeline.get_current_pipeline_inputs()

        for field_name, value_id in self._current_inputs.items():
            self._all_inputs[field_name] = value_id
        self._current_outputs = None

        for stage, steps in pipeline.get_steps_by_stage().items():
            stage_valid = True
            cached_steps = []
            for step_id in steps.keys():
                step_details = pipeline.get_step_details(step_id=step_id)
                if step_details.status == StepStatus.INPUTS_INVALID:
                    stage_valid = False
                    break
                elif step_details.status == StepStatus.INPUTS_READY:
                    job_config = JobConfig(
                        module_type=step_details.step.module_type,
                        module_config=step_details.step.module.config.dict(),
                        inputs=step_details.inputs,
                    )
                    match = self._kiara.job_registry.find_matching_job_record(
                        inputs_manifest=job_config
                    )
                    if match:
                        cached_steps.append(step_id)
            if cached_steps:
                self.process_steps(*cached_steps)
            if not stage_valid:
                break

        self._current_state = None
        self._current_info = None
        self._pipeline_info = None
        return changed

    def process_steps(self, *step_ids: str):

        self.pipeline  # noqa

        if not step_ids:
            output_job_map = self._pipeline_controller.process_pipeline()
        else:
            job_ids = {}
            for step_id in step_ids:
                job_id = self._pipeline_controller.process_step(
                    step_id=step_id, wait=True
                )
                job_ids[step_id] = job_id
            output_job_map = self._pipeline_controller.set_processing_results(
                job_ids=job_ids
            )

        self._job_id_cache.update(output_job_map)

        self._current_outputs = self.pipeline.get_current_pipeline_outputs()
        self._current_state = None
        self._pipeline_info = None
        self._current_info = None

    def _invalidate_pipeline(self):

        self._pipeline_controller.pipeline = None
        self._pipeline = None
        self._pipeline_info = None
        self._current_info = None
        self._current_state = None

    def set_input(self, field_name: str, value: Any):

        self.set_inputs(**{field_name: value})

    def set_inputs(self, **inputs: Any):

        invalid = []
        for k, v in inputs.items():
            if k not in self.pipeline.structure.pipeline_inputs_schema.keys():
                invalid.append(k)
        if invalid:
            raise Exception(
                f"Can't set pipeline inputs, invalid field(s): '{', '.join(invalid)}'. Available inputs: '{', '.join(self.pipeline.structure.pipeline_inputs_schema.keys())}'"
            )

        changed = False
        for k, v in inputs.items():
            # TODO: better equality test?
            if k == self._all_inputs.get(k, None):
                continue
            self._all_inputs[k] = v
            changed = True

        if changed:
            self._current_info = None
            self._current_state = None
            self._current_inputs = None
            self._current_outputs = None
            self._pipeline_info = None
            self._apply_inputs()

    def add_steps(
        self,
        *pipeline_steps: PipelineStep,
        replace_existing: bool = False,
        clear_existing: bool = False,
    ):

        if clear_existing:
            self.clear_steps()

        for step in pipeline_steps:
            if step.step_id in self._steps.keys() and not replace_existing:
                raise Exception(
                    f"Can't add step with id '{step.step_id}': step with that id already exists and 'replace_existing' not set."
                )

        for step in pipeline_steps:
            self._steps[step.step_id] = step
        self._invalidate_pipeline()

    def clear_steps(self, *step_ids: str):

        if not step_ids:
            self._steps.clear()
        else:
            for step_id in step_ids:
                self._steps.pop(step_id, None)

        self._invalidate_pipeline()

    def set_input_alias(self, input_field: str, alias: str):

        self._pipeline_input_aliases[input_field] = alias
        self._invalidate_pipeline()

    def set_output_alias(self, output_field: str, alias: str):
        self._pipeline_output_aliasess[output_field] = alias

    def add_step(
        self,
        operation: str,
        step_id: Union[str, None] = None,
        module_config: Union[None, Mapping[str, Any]] = None,
        input_connections: Union[None, Mapping[str, str]] = None,
        doc: Union[str, DocumentationMetadataModel, None] = None,
        replace_existing: bool = False,
    ) -> PipelineStep:
        """Add a step to the workflows current pipeline structure.

        If no 'step_id' is provided, a unque one will automatically be generated based on the 'module_type' argument.

        Arguments:
            operation: the module or operation name
            step_id: the id of the new step
            module_config: (optional) configuration for the kiara module this step uses
            input_connections: a map with this steps input field name(s) as keys and output field links (format: <step_id>.<output_field_name>) as value(s).
            replace_existing: if set to 'True', this replaces a step with the same id that already exists, otherwise an exception will be thrown
        """

        if step_id is None:
            step_id = find_free_id(
                slugify(operation, separator="_"), current_ids=self._steps.keys()
            )

        if "." in step_id:
            raise Exception(f"Invalid step id '{step_id}': id can't contain '.'.")

        if step_id in self._steps.keys() and not replace_existing:
            raise Exception(
                f"Can't add step with id '{step_id}': step already exists and 'replace_existing' not set."
            )
        elif step_id in self._steps.keys():
            raise NotImplementedError()

        manifest = self._kiara.create_manifest(
            module_or_operation=operation, config=module_config
        )
        module = self._kiara.create_module(manifest=manifest)
        step = PipelineStep(
            step_id=step_id,
            module_type=module.module_type_name,
            module_config=module.config.dict(),
            module_details=KiaraModuleInstance.from_module(module=module),
            doc=doc,
        )
        step._module = module
        self._steps[step_id] = step

        if input_connections:
            for k, v in input_connections.items():
                self.connect_to_inputs(v, f"{step_id}.{k}")

        self._invalidate_pipeline()

        return step

    def connect_fields(self, *fields: Union[Tuple[str, str], str]):

        pairs = []
        current_pair = None
        for field in fields:
            if isinstance(field, str):
                tokens = field.split(".")
                if not len(tokens) == 2:
                    raise Exception(
                        f"Can't connect field '{field}', field name must be in format: <step_id>.<field_name>."
                    )
                if not current_pair:
                    current_pair = [tokens]
                else:
                    if not len(current_pair) == 1:
                        raise Exception(
                            f"Can't connect fields, invalid input(s): {fields}"
                        )
                    current_pair.append(tokens)
                    pairs.append(current_pair)
                    current_pair = None
            else:
                if not len(field) == 2:
                    raise Exception(
                        f"Can't connect fields, field tuples must have length 2: {field}"
                    )
                if current_pair:
                    raise Exception(
                        f"Can't connect fields, dangling single field: {current_pair}"
                    )
                pair = []
                for f in field:
                    tokens = f.split(".")
                    if not len(tokens) == 2:
                        raise Exception(
                            f"Can't connect field '{f}', field name must be in format: <step_id>.<field_name>."
                        )
                    pair.append(tokens)
                pairs.append(pair)

        for pair in pairs:
            self.connect_steps(pair[0][0], pair[0][1], pair[1][0], pair[1][1])

    def connect_steps(
        self,
        source_step: Union[PipelineStep, str],
        source_field: str,
        target_step: Union[PipelineStep, str],
        target_field: str,
    ):

        if isinstance(source_step, str):
            source_step_obj = self.get_step(source_step)
        else:
            source_step_obj = source_step
        if isinstance(target_step, str):
            target_step_obj = self.get_step(target_step)
        else:
            target_step_obj = target_step

        source_step_id = source_step_obj.step_id
        target_step_id = target_step_obj.step_id

        reversed = False

        if source_field not in source_step_obj.module.outputs_schema.keys():
            reversed = True
        if target_field not in target_step_obj.module.inputs_schema.keys():
            reversed = True

        if reversed:
            if target_field not in target_step_obj.module.outputs_schema.keys():
                raise Exception(
                    f"Can't connect steps '{source_step_id}.{source_field}' -> '{target_step_id}.{target_field}': invalid field name(s)."
                )
            if source_field not in source_step_obj.module.inputs_schema.keys():
                raise Exception(
                    f"Can't connect steps '{source_step_id}.{source_field}' -> '{target_step_id}.{target_field}': invalid field name(s)."
                )
        else:
            if target_field not in target_step_obj.module.inputs_schema.keys():
                raise Exception(
                    f"Can't connect steps '{source_step_id}.{source_field}' -> '{target_step_id}.{target_field}': invalid field name(s)."
                )
            if source_field not in source_step_obj.module.outputs_schema.keys():
                raise Exception(
                    f"Can't connect steps '{source_step_id}.{source_field}' -> '{target_step_id}.{target_field}': invalid field name(s)."
                )

        # we rely on the value of input links to always be a dict here
        if not reversed:
            source_addr = StepValueAddress(
                step_id=source_step_id, value_name=source_field
            )
            target_step_obj.input_links.setdefault(target_field, []).append(source_addr)  # type: ignore
        else:
            source_addr = StepValueAddress(
                step_id=target_step_id, value_name=target_field
            )
            source_step_obj.input_links.setdefault(source_field, []).append(source_addr)  # type: ignore

        self._invalidate_pipeline()

    def connect_to_inputs(self, source_field: str, *input_fields: str):

        source_tokens = source_field.split(".")
        if len(source_tokens) != 2:
            raise Exception(
                f"Can't add input link(s): invalid format for provided source '{source_field}', must be string with a single '.' to delimit step-id and output field name."
            )

        source_step = self.get_step(source_tokens[0])
        if source_step is None:
            raise Exception(
                f"Can't add input link(s)': no source step with id '{source_tokens[0]}' exists."
            )

        if source_tokens[1] not in source_step.module.outputs_schema.keys():
            av_fields = ", ".join(source_step.module.outputs_schema.keys())
            raise Exception(
                f"Can't add input link(s): source step with id '{source_step.step_id}' does not have output field '{source_tokens[1]}'. Available field names: {av_fields}."
            )

        source_addr = StepValueAddress(
            step_id=source_step.step_id, value_name=source_tokens[1]
        )

        steps = []
        for input_field in input_fields:
            input_tokens = input_field.split(".")
            if len(input_tokens) != 2:
                raise Exception(
                    f"Can't add input link '{input_field}': invalid format, must be string with a single '.' to delimit step-id and field name."
                )

            step = self.get_step(input_tokens[0])
            if step is None:
                raise Exception(
                    f"Can't add input link '{input_field}': no step with id '{input_tokens[0]}' exists."
                )

            if input_tokens[1] not in step.module.inputs_schema.keys():
                av_fields = ", ".join(step.module.inputs_schema.keys())
                raise Exception(
                    f"Can't add input link '{input_field}': step with id '{input_tokens[0]}' does not have input field '{input_tokens[1]}'. Available field names: {av_fields}."
                )
            steps.append((step, input_tokens[1]))

        for s in steps:
            step, field_name = s
            # we rely on the value of input links to always be a dict here
            step.input_links.setdefault(field_name, []).append(source_addr)  # type: ignore

        self._invalidate_pipeline()

    def get_step(self, step_id: str) -> PipelineStep:

        step = self._steps.get(step_id, None)
        if step is None:
            if self._steps:
                msg = f"Available step ids: {', '.join(self._steps.keys())}"
            else:
                msg = "Workflow does not have any steps (yet)."
            raise Exception(f"No step with id '{step_id}' registered. {msg}")
        return step

    def load_state(
        self, workflow_state_id: Union[str, None] = None
    ) -> Union[None, WorkflowState]:
        """Load a past state.

        If no state id is specified, the latest one that was saved will be used.

        Returns:
            'None' if no state was loaded, otherwise the relevant 'WorkflowState' instance
        """

        if workflow_state_id is None:
            if not self._workflow_details.workflow_states:
                return None
            else:
                workflow_state_id = self._workflow_details.last_state_id

        if workflow_state_id is None:
            raise Exception(
                f"Can't load current state for workflow '{self.workflow_id}': no state available."
            )

        state = self._state_cache.get(workflow_state_id, None)
        if state is not None:
            return state

        state = self._kiara.workflow_registry.get_workflow_state(
            workflow=self.workflow_id, workflow_state_id=workflow_state_id
        )
        assert workflow_state_id == state.instance_id

        self._state_cache[workflow_state_id] = state

        self._all_inputs.clear()
        self._current_inputs = None
        self.clear_steps()
        self._invalidate_pipeline()

        self.add_steps(*state.steps)
        self._pipeline_input_aliases = dict(state.pipeline_config.input_aliases)
        self._pipeline_output_aliasess = dict(state.pipeline_config.output_aliases)

        self.set_inputs(**state.inputs)

        assert self._current_inputs == state.inputs
        self._current_outputs = state.pipeline_info.pipeline_details.pipeline_outputs
        self._pipeline_info = state.pipeline_info
        self._current_state = state
        self._current_info = None

        return state

    @property
    def all_states(self) -> Mapping[str, WorkflowState]:

        missing = []
        for state_id in self.details.workflow_states.values():
            if state_id not in self._state_cache.keys():
                missing.append(state_id)

        if missing:
            # TODO: only request missing ones?
            all_states = self._kiara.workflow_registry.get_all_states_for_workflow(
                workflow=self.workflow_id
            )
            self._state_cache.update(all_states)

        return self._state_cache

    @property
    def info(self) -> WorkflowInfo:

        if self._current_info is not None:
            return self._current_info

        self._current_info = WorkflowInfo.create_from_workflow(workflow=self)
        return self._current_info

    @property
    def pipeline_info(self) -> PipelineInfo:

        if self._pipeline_info is not None:
            return self._pipeline_info

        self._pipeline_info = PipelineInfo.create_from_pipeline(
            kiara=self._kiara, pipeline=self.pipeline
        )
        return self._pipeline_info

    def snapshot(self, save: bool = True) -> WorkflowState:

        state = self.current_state

        if not self._is_stored:
            aliases = []
            if self._workflow_alias:
                aliases.append(self._workflow_alias)
            self._kiara.workflow_registry.register_workflow(
                workflow_details=self._workflow_details, workflow_aliases=aliases
            )
            self._is_stored = True

        if save:
            for value in state.inputs.values():
                self._kiara.data_registry.store_value(value=value)

            for value in self.current_outputs.values():
                if value in [NOT_SET_VALUE_ID, NONE_VALUE_ID]:
                    continue
                self._kiara.data_registry.store_value(value=value)
                job_id = self._job_id_cache[value]
                try:
                    self._kiara.job_registry.store_job_record(job_id=job_id)
                except Exception as e:
                    print(e)

            self._workflow_details = self._kiara.workflow_registry.add_workflow_state(
                workflow=self._workflow_details, workflow_state=state
            )

        return state

    def create_renderable(self, **config: Any):

        if not self._steps:
            return "Invalid workflow: no steps set yet."

        return self.info.create_renderable(**config)
