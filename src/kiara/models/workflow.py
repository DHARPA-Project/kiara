# -*- coding: utf-8 -*-
import datetime
import uuid
from dag_cbor.encoding import EncodableType
from pydantic import BaseModel, Field, PrivateAttr, validator
from typing import Dict, Iterable, List, Mapping, Union, TYPE_CHECKING

from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.pipeline import PipelineConfig, PipelineStep
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.registries.ids import ID_REGISTRY

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.workflows import Workflow


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

    current_state: Union[uuid.UUID, None] = Field(
        description="A reference to the current state of this workflow.", default=None
    )

    _kiara: Union["Kiara", None] = PrivateAttr(default=None)

    @validator("documentation", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    def create_workflow_state(
        self,
        steps: Union[Iterable[PipelineStep], None] = None,
        inputs: Union[Mapping[str, uuid.UUID], None] = None,
    ) -> "WorkflowState":

        workflow_state_id = ID_REGISTRY.generate(
            comment=f"new workflow state for workflow: {self.workflow_id}"
        )

        if not steps:
            steps = []
        else:
            steps = list(steps)

        if inputs is None:
            inputs = {}
        else:
            inputs = dict(inputs)

        state = WorkflowState(
            workflow_state_id=workflow_state_id,
            workflow_id=self.workflow_id,
            steps=steps,
            inputs=inputs,
        )
        return state


class WorkflowState(KiaraModel):
    @classmethod
    def create_snapshot(cls, workflow: "Workflow") -> "WorkflowState":

        raise NotImplementedError()

    workflow_state_id: uuid.UUID = Field(
        description="The globally unique uuid for this workflow state."
    )
    workflow_id: uuid.UUID = Field(
        description="The id of the workflow this state is connected to."
    )
    steps: List[PipelineStep] = Field(
        description="The current steps in the workflow.", default_factory=list
    )
    inputs: Dict[str, uuid.UUID] = Field(
        description="The current (pipeline) input values.", default_factory=dict
    )
    # outputs: Dict[str, uuid.UUID] = Field(description="The current (pipeline) output values.", default_factory=dict)
    created: datetime.datetime = Field(
        description="The time this snapshot was created.",
        default_factory=datetime.datetime.now,
    )
    parent: Union[uuid.UUID, None] = Field(
        description="The id of the state that preceeded this one (if applicable).",
        default=None,
    )

    _pipeline_config: Union[PipelineConfig, None] = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> EncodableType:
        return self.structure.instance_cid

    @property
    def pipeline_config(self):

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

        workflow_state_id = ID_REGISTRY.generate(
            comment=f"new workflow state for workflow: {self.workflow_id}"
        )

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
            workflow_state_id=workflow_state_id,
            workflow_id=self.workflow_id,
            steps=steps,
            inputs=inputs,
        )
        return state


class WorkflowStateFilter(BaseModel):

    earliest: Union[datetime.datetime, None] = Field(
        description="The earliest state to be considered.", default=None
    )
    latest: Union[datetime.datetime, None] = Field(
        description="The latest state to be considered.", default=None
    )
    max_states: Union[int, None] = Field(
        description="Max amount of states to return (using X oldest states).",
        default=None,
    )


# class Workflow(KiaraModel):
#
#     _kiara_model_id = "instance.workflow"
#
#     workflow_id: uuid.UUID = Field(
#         description="The globally unique uuid for this workflow."
#     )
#     documentation: DocumentationMetadataModel = Field(description="A description for this workflow.", default_factory=DocumentationMetadataModel.create)
#     states: Dict[uuid.UUID, WorkflowState] = Field(description="A history of workflow states.", default_factory=dict)
#
#
#     @validator("documentation", pre=True)
#     def validate_doc(cls, value):
#         return DocumentationMetadataModel.create(value)
