# -*- coding: utf-8 -*-
import datetime
import uuid
from dag_cbor.encoding import EncodableType
from pydantic import Field, PrivateAttr, validator
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Type, Union

from kiara.interfaces.python_api.models.info import InfoItemGroup, ItemInfo
from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.module.pipeline import PipelineConfig, PipelineStep
from kiara.models.module.pipeline.pipeline import Pipeline, PipelineInfo
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import is_jupyter

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.interfaces.python_api.workflow import Workflow


class WorkflowState(KiaraModel):
    @classmethod
    def create_from_workflow(self, workflow: "Workflow"):

        steps = list(workflow._steps.values())
        inputs = dict(workflow.current_inputs)
        info = PipelineInfo.create_from_pipeline(
            kiara=workflow._kiara, pipeline=workflow.pipeline
        )
        info._kiara = workflow._kiara

        ws = WorkflowState(steps=steps, inputs=inputs, pipeline_info=info)
        ws._kiara = workflow._kiara
        ws.pipeline_info._kiara = workflow._kiara
        return ws

    steps: List[PipelineStep] = Field(
        description="The current steps in the workflow.", default_factory=list
    )
    inputs: Dict[str, uuid.UUID] = Field(
        description="The current (pipeline) input values.", default_factory=dict
    )
    pipeline_info: PipelineInfo = Field(
        description="Details about the pipeline and its state."
    )

    _pipeline: Union[Pipeline, None] = PrivateAttr(default=None)
    _kiara: "Kiara" = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> EncodableType:
        return {
            "pipeline_info": self.pipeline_info.instance_cid,
            "inputs": {k: str(v) for k, v in self.inputs.items()},
        }

    def set_inputs(self, **inputs: uuid.UUID):

        for k, v in inputs.items():
            if k in self.pipeline_config.structure.pipeline_inputs_schema.keys():
                self.inputs[k] = v

    @property
    def pipeline_config(self) -> PipelineConfig:

        return self.pipeline_info.pipeline_structure.pipeline_config

    @property
    def pipeline_structure(self) -> PipelineStructure:
        return self.pipeline_info.pipeline_structure

    def create_renderable(self, **config: Any) -> RenderableType:

        in_panel = config.get("in_panel", None)
        if in_panel is None:
            if is_jupyter():
                in_panel = True
            else:
                in_panel = False
        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
        table.add_column("property", style="i")
        table.add_column("value")
        table.add_row("state id", self.instance_id)

        self.pipeline_info._fill_table(table=table, config=config)

        if in_panel:
            return Panel(table)
        else:
            return table


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

    _kiara: Union["Kiara", None] = PrivateAttr(default=None)
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


class WorkflowInfo(ItemInfo):

    _kiara_model_id = "info.workflow"

    @classmethod
    def create_from_workflow(cls, workflow: "Workflow"):

        wf_info = WorkflowInfo.construct(
            type_name=str(workflow.workflow_id),
            workflow_details=workflow.details,
            workflow_states=workflow.all_states,
            pipeline_info=workflow.pipeline_info,
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

        in_panel = config.get("in_panel", None)
        if in_panel is None:
            if is_jupyter():
                in_panel = True
            else:
                in_panel = False

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
                "documentation",
                Panel(self.documentation.create_renderable(), box=box.SIMPLE),
            )
        if include_authors:
            table.add_row("author(s)", self.authors.create_renderable(**config))
        if include_context:
            table.add_row("context", self.context.create_renderable(**config))
        if include_history:
            history_table = Table(show_header=False, box=box.SIMPLE)
            history_table.add_column("date", style="i")
            history_table.add_column("id")
            for d, s_id in self.workflow_details.workflow_states.items():
                history_table.add_row(str(d), s_id)
            table.add_row("states", history_table)

        if include_current_state:
            current_state_id = (
                "-- n/a --"
                if not self.workflow_details.current_state
                else self.workflow_details.current_state
            )
            table.add_row("current state id", current_state_id)
            table.add_row(
                "current state details", self.pipeline_info.create_renderable(**config)
            )

        if in_panel:
            return Panel(table)
        else:
            return table


class WorkflowGroupInfo(InfoItemGroup):

    _kiara_model_id = "info.workflows"

    @classmethod
    def base_info_class(cls) -> Type[ItemInfo]:
        return WorkflowInfo

    @classmethod
    def create_from_workflows(
        cls,
        *items: "Workflow",
        group_title: Union[str, None] = None,
        alias_map: Union[None, Mapping[str, uuid.UUID]] = None
    ) -> "WorkflowGroupInfo":

        workflow_infos = {
            str(w.workflow_id): WorkflowInfo.create_from_workflow(workflow=w)
            for w in items
        }
        if alias_map is None:
            alias_map = {}
        workflow_group_info = cls.construct(
            group_title=group_title, item_infos=workflow_infos, aliases=alias_map
        )
        return workflow_group_info

    item_infos: Mapping[str, WorkflowInfo] = Field(
        description="The workflow infos objects for each workflow."
    )
    aliases: Mapping[str, uuid.UUID] = Field(
        description="The available aliases.", default_factory=dict
    )

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(box=box.SIMPLE, show_header=True)
        table.add_column("alias(es)", style="i")
        table.add_column("workflow_id")
        table.add_column("# steps")
        table.add_column("# stages")
        table.add_column("# states")
        table.add_column("description")

        for workflow_id, wf in self.item_infos.items():

            aliases = [k for k, v in self.aliases.items() if str(v) == workflow_id]
            steps = len(wf.pipeline_info.pipeline_structure.steps)
            stages = len(wf.pipeline_info.pipeline_structure.processing_stages)
            states = len(wf.workflow_states)

            if not aliases:
                alias_str = ""
            else:
                alias_str = ", ".join(aliases)
            table.add_row(
                alias_str,
                workflow_id,
                str(steps),
                str(stages),
                str(states),
                wf.documentation.description,
            )

        return table
