# -*- coding: utf-8 -*-
import datetime
import uuid
from dag_cbor.encoding import EncodableType
from pydantic import Field, PrivateAttr, validator
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Union

from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.info import ItemInfo
from kiara.models.module.pipeline import PipelineConfig, PipelineStep
from kiara.models.module.pipeline.pipeline import PipelineInfo
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.registries.ids import ID_REGISTRY

if TYPE_CHECKING:
    from kiara.interfaces.python_api.workflow import Workflow


class WorkflowState(KiaraModel):
    # @classmethod
    # def create_snapshot(cls, workflow: "Workflow") -> "WorkflowState":
    #
    #     raise NotImplementedError()

    # workflow_state_id: uuid.UUID = Field(
    #     description="The globally unique uuid for this workflow state."
    # )
    # workflow_id: uuid.UUID = Field(
    #     description="The id of the workflow this state is connected to."
    # )
    steps: List[PipelineStep] = Field(
        description="The current steps in the workflow.", default_factory=list
    )
    inputs: Dict[str, uuid.UUID] = Field(
        description="The current (pipeline) input values.", default_factory=dict
    )
    # outputs: Dict[str, uuid.UUID] = Field(description="The current (pipeline) output values.", default_factory=dict)
    # created: datetime.datetime = Field(
    #     description="The time this snapshot was created.",
    #     default_factory=datetime.datetime.now,
    # )
    # parent: Union[uuid.UUID, None] = Field(
    #     description="The id of the state that preceeded this one (if applicable).",
    #     default=None,
    # )
    _pipeline_config: Union[PipelineConfig, None] = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> EncodableType:
        return {
            "structure": self.structure.instance_cid,
            "inputs": {k: str(v) for k, v in self.inputs.items()},
        }

    def set_inputs(self, **inputs: uuid.UUID):

        for k, v in inputs.items():
            if k in self.pipeline_config.structure.pipeline_inputs_schema.keys():
                self.inputs[k] = v

    @property
    def pipeline_config(self) -> PipelineConfig:

        if self._pipeline_config is not None:
            return self._pipeline_config

        self._pipeline_config = PipelineConfig.from_config(
            pipeline_name="__workflow__", data={"steps": self.steps}
        )
        return self._pipeline_config

    @property
    def structure(self) -> PipelineStructure:
        return self.pipeline_config.structure

    def create_workflow_state(
        self,
        steps: Iterable[PipelineStep] = None,
        inputs: Union[Mapping[str, uuid.UUID], None] = None,
    ) -> "WorkflowState":

        # workflow_state_id = ID_REGISTRY.generate(
        #     comment=f"new workflow state for workflow: {self.instance_id}"
        # )

        if not steps:
            steps = self.steps
        else:
            # TODO: augment steps instead of replace?
            steps = list(steps)

        if inputs is None:
            inputs = {}
        else:
            inputs = dict(inputs)

        for k, v in self.inputs.items():
            if k not in inputs.keys():
                inputs[k] = v

        state = WorkflowState(
            steps=steps,
            inputs=inputs,
        )
        return state


class WorkflowDetails(KiaraModel):
    _kiara_model_id = "instance.workflow"

    workflow_id: uuid.UUID = Field(
        description="The globally unique uuid for this workflow.",
        default_factory=ID_REGISTRY.generate,
    )
    documentation: DocumentationMetadataModel = Field(
        description="A description for this workflow.",
        default_factory=DocumentationMetadataModel.create,
    )
    authors: AuthorsMetadataModel = Field(
        description="The author(s) of this workflow.",
        default_factory=AuthorsMetadataModel,
    )
    context: ContextMetadataModel = Field(
        description="Workflow context details.", default_factory=ContextMetadataModel
    )
    current_state: Union[str, None] = Field(
        description="A reference to the current state of this workflow.", default=None
    )
    workflow_states: Dict[datetime.datetime, str] = Field(
        description="A history of all the states of this workflow.",
        default_factory=dict,
    )

    # _kiara: Union["Kiara", None] = PrivateAttr(default=None)
    # _last_update: datetime.datetime = PrivateAttr(default_factory=datetime.datetime.now)

    @validator("documentation", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    @property
    def last_state_id(self) -> Union[None, str]:

        if not self.workflow_states:
            return None
        last_date = max(self.workflow_states.keys())
        workflow_state_id = self.workflow_states[last_date]
        return workflow_state_id

    def create_workflow_state(
        self,
        steps: Union[Iterable[PipelineStep], None] = None,
        inputs: Union[Mapping[str, uuid.UUID], None] = None,
    ) -> "WorkflowState":

        if not steps:
            steps = []
        else:
            steps = list(steps)

        if inputs is None:
            inputs = {}
        else:
            inputs = dict(inputs)

        state = WorkflowState(
            steps=steps,
            inputs=inputs,
        )
        return state


class WorkflowInfo(ItemInfo):

    _kiara_model_id = "info.workflow"

    @classmethod
    def create_from_workflow(cls, workflow: "Workflow"):

        pipeline_info = PipelineInfo.create_from_pipeline(
            kiara=workflow._kiara, pipeline=workflow.pipeline
        )
        wf_info = WorkflowInfo.construct(
            type_name=str(workflow.workflow_id),
            workflow_details=workflow.details,
            workflow_states=workflow.all_states,
            pipeline_info=pipeline_info,
            documentation=workflow.details.documentation,
            authors=workflow.details.authors,
            context=workflow.details.context,
        )
        return wf_info

    @classmethod
    def category_name(cls) -> str:
        return "workflow"

    workflow_details: WorkflowDetails = Field(description="The workflow details.")
    workflow_states: Mapping[str, WorkflowState] = Field(
        description="All states for this workflow."
    )
    pipeline_info: PipelineInfo = Field(
        description="The current state of the workflows' pipeline."
    )

    def create_renderable(self, **config: Any) -> RenderableType:

        include_doc = config.get("include_doc", True)
        include_authors = config.get("include_authors", True)
        include_context = config.get("include_context", True)
        include_history = config.get("include_history", True)
        include_current_state = config.get("include_current_state", True)

        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
        table.add_column("property", style="i")
        table.add_column("value")

        if include_doc:
            table.add_row(
                "Documentation",
                Panel(self.documentation.create_renderable(), box=box.SIMPLE),
            )
        if include_authors:
            table.add_row("Author(s)", self.authors.create_renderable(**config))
        if include_context:
            table.add_row("Context", self.context.create_renderable(**config))
        if include_history:
            pass

        if include_current_state:
            table.add_row(
                "Current state", self.pipeline_info.create_renderable(**config)
            )
        return table
