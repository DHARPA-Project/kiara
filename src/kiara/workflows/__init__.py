# -*- coding: utf-8 -*-
import datetime
import structlog
import uuid
from dag_cbor.encoding import EncodableType
from pydantic import Field
from rich import box
from rich.table import Table
from slugify import slugify
from typing import Any, Dict, List, Mapping, Optional

from kiara import Kiara
from kiara.models import KiaraModel
from kiara.models.events.pipeline import PipelineDetails, PipelineEvent
from kiara.models.module.jobs import ExecutionContext
from kiara.models.module.manifest import Manifest
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.controller import SinglePipelineController
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.models.values.value import ValueMap, ValueMapReadOnly
from kiara.utils import find_free_id, is_debug

logger = structlog.getLogger()


class WorkflowState(KiaraModel):

    pipeline_config: PipelineConfig = Field(
        description="The current pipeline config/structure."
    )
    inputs: Dict[str, uuid.UUID] = Field(description="The current input values.")
    created: datetime.datetime = Field(
        description="The time this snapshot was created.",
        default_factory=datetime.datetime.now,
    )

    def _retrieve_data_to_hash(self) -> EncodableType:
        return self.structure.instance_cid

    @property
    def structure(self) -> PipelineStructure:
        return self.pipeline_config.structure


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
        kiara: Kiara,
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
    def __init__(self, workflow_alias: str, kiara: Kiara):

        self._kiara: Kiara = kiara
        self._pipeline_controller: WorkflowPipelineController = (
            WorkflowPipelineController(kiara=self._kiara)
        )
        self._workflow_alias: str = workflow_alias
        self._steps: Dict[str, Manifest] = {}
        self._input_links: Dict[str, List[str]] = {}

        self._pipeline_config: Optional[PipelineConfig] = None
        self._all_inputs: Dict[str, Any] = {}
        self._current_inputs: Optional[Dict[str, Any]] = None
        self._last_outputs: Dict[str, uuid.UUID] = {}
        self._current_outputs: Optional[Dict[str, uuid.UUID]] = None
        self._pipeline_manifest: Optional[Manifest] = None
        self._pipeline_details: Optional[PipelineDetails] = None
        self._pipeline: Optional[Pipeline] = None

        self._execution_context: ExecutionContext = ExecutionContext()

        self._snapshots: List[WorkflowState] = []

    @property
    def workflow_alias(self) -> str:
        return self._workflow_alias

    @property
    def current_inputs(self) -> Dict[str, uuid.UUID]:

        if self._current_inputs is not None:
            return self._current_inputs

        current = {}
        for (
            field_name,
            schema,
        ) in self.pipeline.structure.pipeline_inputs_schema.items():
            if field_name in self._all_inputs.keys():
                current[field_name] = self._all_inputs[field_name]
        self._current_inputs = current
        return self._current_inputs

    @property
    def current_outputs(self) -> Dict[str, uuid.UUID]:

        if self._current_outputs is not None:
            return self._current_outputs

        self.apply()
        self._current_outputs = self.pipeline.get_current_pipeline_outputs()
        return self._current_outputs

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

    def snapshot(self):

        self._validate()

        snap = WorkflowState(
            pipeline_config=self.pipeline_config, inputs=self.current_inputs
        )
        self._snapshots.append(snap)

    def apply(self) -> PipelineDetails:

        self.pipeline.set_pipeline_inputs(inputs=self.current_inputs)
        self._pipeline_controller.process_pipeline()

        return self.pipeline_details

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
            pipeline_name=self.workflow_alias,
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

    def add_step(
        self,
        module_type: str,
        step_id: Optional[str] = None,
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

        table.add_row("workflow alias", self.workflow_alias)
        table.add_row("pipeline", self.pipeline.create_renderable(**config))

        return table
