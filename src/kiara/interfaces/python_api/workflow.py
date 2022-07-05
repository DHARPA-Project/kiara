# -*- coding: utf-8 -*-
import structlog
import uuid
from rich import box
from rich.table import Table
from slugify import slugify
from typing import TYPE_CHECKING, Any, Dict, Mapping, Union

from kiara.defaults import NONE_VALUE_ID, NOT_SET_VALUE_ID
from kiara.models import KiaraModel
from kiara.models.events.pipeline import ChangedValue, PipelineEvent
from kiara.models.module import KiaraModuleClass
from kiara.models.module.jobs import ExecutionContext, JobConfig
from kiara.models.module.pipeline import (
    PipelineConfig,
    PipelineStep,
    StepStatus,
    StepValueAddress,
)
from kiara.models.module.pipeline.controller import SinglePipelineController
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.values.value_schema import ValueSchema
from kiara.models.workflow import WorkflowDetails, WorkflowState
from kiara.utils import find_free_id, is_debug

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
                        if is_debug():
                            import traceback

                            traceback.print_exc()
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
        except Exception:
            if is_debug():
                import traceback

                traceback.print_exc()

        finally:
            self._is_running = False

        log.debug("execute_finished.pipeline")
        return result


class WorkflowStatus(KiaraModel):
    pass


class Workflow(object):
    def __init__(self, kiara: "Kiara", workflow: Union[uuid.UUID, str]):

        self._kiara: Kiara = kiara

        self._execution_context: ExecutionContext = ExecutionContext()
        self._pipeline_controller: WorkflowPipelineController = (
            WorkflowPipelineController(kiara=self._kiara)
        )

        # TODO: create if not exists?
        self._workflow_details: WorkflowDetails = (
            self._kiara.workflow_registry.get_workflow_details(workflow=workflow)
        )

        self._all_inputs: Dict[str, Any] = {}
        self._current_inputs: Union[Dict[str, uuid.UUID], None] = None
        self._current_outputs: Union[Dict[str, uuid.UUID], None] = None

        self._steps: Dict[str, PipelineStep] = {}

        # self._current_state_cid: Union[None, CID] = None
        self._state_cache: Dict[str, WorkflowState] = {}

        self._job_id_cache: Dict[uuid.UUID, uuid.UUID] = {}
        """Cache to save job ids per output value(s), in order to save jobs if output values are saved."""

        self._pipeline: Union[Pipeline, None] = None

        if self._workflow_details.workflow_states:
            self.load_state()

    @property
    def workflow_id(self) -> uuid.UUID:
        return self._workflow_details.workflow_id

    # @property
    # def current_state(self) -> WorkflowState:
    #
    #     if self._current_state_cid is not None:
    #         return self._state_cache[self._current_state_cid]
    #
    #     workflow_state_id = ID_REGISTRY.generate(comment="new workflow state")
    #     if self._steps is None:
    #         self._steps = {}
    #
    #     state = WorkflowState(workflow_state_id=workflow_state_id, workflow_id=self.workflow_id, steps=list(self._steps.values()))
    #     assert state.instance_cid not in self._state_cache.keys()
    #     self._state_cache[state.instance_cid] = state
    #     self._current_state_cid = state.instance_cid
    #     return state

    @property
    def details(self) -> WorkflowDetails:
        return self._workflow_details

    @property
    def current_inputs_schema(self) -> Mapping[str, ValueSchema]:
        return self.pipeline.structure.pipeline_inputs_schema

    @property
    def current_outputs_schema(self) -> Mapping[str, ValueSchema]:
        return self.pipeline.structure.pipeline_outputs_schema

    @property
    def current_inputs(self) -> Mapping[str, uuid.UUID]:

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
    def pipeline(self) -> Pipeline:

        if self._pipeline is not None:
            return self._pipeline

        if not self._steps:
            raise Exception("Can't assemble pipeline, no steps set (yet).")

        pipeline_config = PipelineConfig.from_config(
            pipeline_name="__workflow__", data={"steps": self._steps.values()}
        )
        structure = pipeline_config.structure
        self._pipeline = Pipeline(structure=structure, kiara=self._kiara)
        self._pipeline_controller.pipeline = self._pipeline
        return self._pipeline

    def _apply_inputs(self) -> Mapping[str, Mapping[str, Mapping[str, ChangedValue]]]:

        inputs_to_set = {}
        for field_name, value in self._all_inputs.items():
            if value in [NONE_VALUE_ID, NOT_SET_VALUE_ID]:
                continue
            if field_name in self.pipeline.structure.pipeline_inputs_schema.keys():
                inputs_to_set[field_name] = value

        logger.debug(
            "workflow.apply_inputs",
            workflow_id=str(self.workflow_id),
            keys=", ".join(inputs_to_set.keys()),
        )

        pipeline = self.pipeline

        changed: Mapping[
            str, Mapping[str, Mapping[str, ChangedValue]]
        ] = self.pipeline.set_pipeline_inputs(inputs=inputs_to_set)
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

        return changed

    def process_steps(self, *step_ids: str):

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

    def _invalidate_pipeline(self):

        self._pipeline_controller.pipeline = None
        self._pipeline = None

    def set_inputs(self, **inputs: Any):

        changed = False

        for k, v in inputs.items():
            # TODO: better equality test?
            if k == self._all_inputs.get(k, None):
                continue
            self._all_inputs[k] = v
            changed = True

        if changed:
            self._apply_inputs()

    def add_steps(self, *pipeline_steps: PipelineStep, replace_existing: bool = False):

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

    def create_step(
        self,
        module_type: str,
        step_id: Union[str, None] = None,
        module_config: Union[None, Mapping[str, Any]] = None,
        input_links: Union[None, Mapping[str, str]] = None,
        replace_existing: bool = False,
    ) -> str:

        if step_id is None:
            step_id = find_free_id(
                slugify(module_type, separator="_"), current_ids=self._steps.keys()
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
            module_or_operation=module_type, config=module_config
        )
        module = self._kiara.create_module(manifest=manifest)
        step = PipelineStep(
            step_id=step_id,
            module_type=module.module_type_name,
            module_config=module.config.dict(),
            module_details=KiaraModuleClass.from_module(module=module),
        )
        step._module = module
        self._steps[step_id] = step

        if input_links:
            for k, v in input_links.items():
                self.add_input_link(v, k)

        self._invalidate_pipeline()

        return step_id

    def add_input_link(self, source_field: str, *input_fields: str):

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

    def get_step(self, step_id: str) -> Union[None, PipelineStep]:

        return self._steps.get(step_id, None)

    def load_state(
        self, workflow_state_id: Union[str, None] = None
    ) -> Union[None, WorkflowState]:

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

        self.clear_steps()
        self.add_steps(*state.steps)
        self.set_inputs(**state.inputs)

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

    def snapshot(self, save: bool = False) -> WorkflowState:

        state = WorkflowState(
            steps=list(self._steps.values()), inputs=dict(self.current_inputs)
        )

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

        table = Table(box=box.SIMPLE, show_header=False)
        table.add_column("key", style="i")
        table.add_column("value")

        table.add_row("workflow id", str(self.workflow_id))
        if self._steps:
            table.add_row("pipeline", self.pipeline.create_renderable(**config))
        else:
            table.add_row("pipeline", "-- no steps (yet) --")

        return table
