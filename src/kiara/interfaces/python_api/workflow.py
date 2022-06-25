# -*- coding: utf-8 -*-
import structlog
import uuid
from rich import box
from rich.table import Table
from slugify import slugify
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Union

from kiara.models.events.pipeline import PipelineDetails, PipelineEvent
from kiara.models.module.jobs import ExecutionContext
from kiara.models.module.manifest import Manifest
from kiara.models.module.pipeline import PipelineConfig, PipelineStep
from kiara.models.module.pipeline.controller import SinglePipelineController
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.values.value import ValueMap, ValueMapReadOnly
from kiara.models.workflow import WorkflowDetails, WorkflowState
from kiara.registries.ids import ID_REGISTRY
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
        # dbg(event.dict())

    def process_pipeline(self):

        log = logger.bind(pipeline_id=self.pipeline.pipeline_id)
        if self._is_running:
            log.debug(
                "ignore.pipeline_process",
                reason="Pipeline already running.",
            )
            raise Exception("Pipeline already running.")

        log.debug("execute.pipeline")
        self._is_running = True
        try:
            for idx, stage in enumerate(
                self.pipeline.structure.processing_stages, start=1
            ):

                log.debug(
                    "execute.pipeline.stage",
                    stage=idx,
                )

                job_ids = {}
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
                        return False

                self.set_processing_results(job_ids=job_ids)
                log.debug(
                    "execute_finished.pipeline.stage",
                    stage=idx,
                )

        finally:
            self._is_running = False

        log.debug("execute_finished.pipeline")


class Workflow(object):
    def __init__(self, kiara: "Kiara", workflow_id: uuid.UUID):

        self._kiara: Kiara = kiara
        self._workflow_id: uuid.UUID = workflow_id

        self._execution_context: ExecutionContext = ExecutionContext()
        self._pipeline_controller: WorkflowPipelineController = (
            WorkflowPipelineController(kiara=self._kiara)
        )

        self._workflow_details: Union[WorkflowDetails, None] = None

        self._all_inputs: Dict[str, Any] = {}

        self._steps: Union[Dict[str, Manifest], None] = None
        self._input_links: Union[Dict[str, List[str]], None] = None
        self._current_inputs: Union[Dict[str, Any], None] = None
        self._pipeline_config: Union[PipelineConfig, None] = None
        self._last_outputs: Dict[str, uuid.UUID] = {}
        self._current_outputs: Union[Dict[str, uuid.UUID], None] = None

        self._pipeline_manifest: Union[Manifest, None] = None
        self._pipeline_details: Union[PipelineDetails, None] = None
        self._pipeline: Union[Pipeline, None] = None

        self._snapshots: Dict[uuid.UUID, WorkflowState] = None

    @property
    def workflow_id(self) -> uuid.UUID:
        return self._workflow_id

    @property
    def details(self) -> WorkflowDetails:
        if self._workflow_details is None:
            self._workflow_details = self._kiara.workflow_registry.get_workflow_details(
                workflow=self._workflow_id
            )
        return self._workflow_details

    def set_inputs(self, **inputs: Any):
        self._all_inputs.update(**inputs)
        self._current_inputs = None
        self._current_outputs = None
        self._pipeline_details = None

    def _invalidate(self):
        self._pipeline_config = None
        self._pipeline_manifest = None
        self._pipeline_details = None
        self._pipeline = None
        self._pipeline_controller.pipeline = None
        self._current_inputs = None
        self._current_outputs = None

    def _validate(self):

        self.current_inputs  # noqa

    @property
    def current_inputs(self) -> Dict[str, uuid.UUID]:

        if self._current_inputs is not None:
            return self._current_inputs

        to_change = {}
        for (
            field_name,
            schema,
        ) in self.pipeline.structure.pipeline_inputs_schema.items():
            if field_name in self._all_inputs.keys():
                to_change[field_name] = self._all_inputs[field_name]

        changed = self.pipeline.set_pipeline_inputs(inputs=to_change)
        self._current_inputs = self.pipeline.get_current_pipeline_inputs()
        return self._current_inputs

    @property
    def current_outputs(self) -> Dict[str, uuid.UUID]:

        if self._current_outputs is not None:
            return self._current_outputs

        self.apply()
        self._current_outputs = self.pipeline.get_current_pipeline_outputs()
        return self._current_outputs

    def snapshot(self) -> WorkflowState:

        self._validate()

        workflow_state_id = ID_REGISTRY.generate(comment="new workflow state")

        snap = WorkflowState(
            workflow_id=self.workflow_id,
            workflow_state_id=workflow_state_id,
            steps=self.pipeline_config.steps,
            inputs=self.current_inputs,
            outputs=self.current_outputs,
        )
        self._snapshots[snap.workflow_state_id] = snap

        return snap

    def apply(self) -> PipelineDetails:

        self.pipeline.set_pipeline_inputs(inputs=self.current_inputs)
        self._pipeline_controller.process_pipeline()

        return self.pipeline_details

    def load_state(
        self, workflow_state_id: Union[uuid.UUID, None] = None
    ) -> WorkflowState:

        if workflow_state_id is None:
            workflow_state_id = max(
                self._snapshots, key=lambda x: self._snapshots[x].created
            )

        state = self._snapshots.get(workflow_state_id, None)
        if state is None:
            raise Exception(
                f"Can't load state with id '{workflow_state_id}': no state with that id registered."
            )

        self._steps = {}
        self._invalidate()
        self.add_steps(*state.steps)
        self.set_inputs(**state.inputs)
        self.apply()

        return state

    @property
    def current_output_values(self) -> ValueMap:

        return ValueMapReadOnly.create_from_ids(
            data_registry=self._kiara.data_registry, **self.current_outputs
        )

    def get_outputs(self, step_id: str = "pipeline") -> Mapping[str, uuid.UUID]:

        if step_id == "pipeline":
            pipeline_outputs = self.pipeline.get_current_pipeline_outputs()
            return pipeline_outputs
        else:
            step_outputs = self.pipeline.get_current_step_outputs(step_id=step_id)
            return step_outputs

    @property
    def pipeline_config(self) -> PipelineConfig:

        if self._pipeline_config is not None:
            return self._pipeline_config

        all_steps = []

        for step_id, manifest in self._steps.items():
            _step_data: Dict[str, Any] = dict(manifest.manifest_data)
            _step_data["step_id"] = step_id
            input_links = {}
            for source, target in self._input_links.items():
                if source.startswith(f"{step_id}."):
                    _, field_name = source.split(".", maxsplit=1)
                    input_links[field_name] = target
            _step_data["input_links"] = input_links
            all_steps.append(_step_data)

        self._pipeline_config = PipelineConfig.from_config(
            pipeline_name=str(self.workflow_id),
            data={"steps": all_steps},
            kiara=self._kiara,
            execution_context=self._execution_context,
        )
        return self._pipeline_config

    @property
    def pipeline_manifest(self) -> Manifest:

        if self._pipeline_manifest is not None:
            return self._pipeline_manifest

        self._pipeline_manifest = self._kiara.create_manifest(
            module_or_operation="pipeline", config=self.pipeline_config.dict()
        )
        return self._pipeline_manifest

    @property
    def pipeline(self) -> Pipeline:
        if self._pipeline is not None:
            return self._pipeline

        self._pipeline = Pipeline(
            structure=self.pipeline_config.structure, kiara=self._kiara
        )
        self._pipeline_controller.pipeline = self._pipeline
        return self._pipeline

    @property
    def pipeline_details(self) -> PipelineDetails:
        if self._pipeline_details is not None:
            return self._pipeline_details

        self._pipeline_details = self.pipeline.get_pipeline_details()
        return self._pipeline_details

    # @property
    # def pipeline_module(self) -> KiaraModule:
    #
    #     if self._pipeline_module is not None:
    #         return self._pipeline_module
    #
    #     self._pipeline_module = self._kiara.create_module(  # type: ignore
    #         manifest=self.pipeline_manifest
    #     )
    #     return self._pipeline_module  # type: ignore

    def add_steps(self, *pipeline_steps: PipelineStep):
        for step in pipeline_steps:
            self.add_step(
                module_type=step.module_type,
                module_config=step.module_config,
                step_id=step.step_id,
                replace_existing=False,
            )

        for step in pipeline_steps:
            for k, v in step.input_links.items():
                assert len(v) == 1
                self.add_input_link(input_field=k, source=v[0].alias)

    def add_step(
        self,
        module_type: str,
        step_id: Union[str, None] = None,
        module_config: Mapping[str, Any] = None,
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
        self._steps[step_id] = manifest
        self._invalidate()

        return step_id

    def add_input_link(self, input_field: str, source: str):

        self._input_links[input_field] = [source]
        self._invalidate()

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
