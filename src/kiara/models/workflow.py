# -*- coding: utf-8 -*-
import datetime
import uuid
from dag_cbor.encoding import EncodableType
from pydantic import Field, PrivateAttr, validator
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.syntax import Syntax
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
from kiara.models.values.value import ValueMap
from kiara.utils import is_jupyter
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.interfaces.python_api.workflow import Workflow


class WorkflowState(KiaraModel):
    @classmethod
    def create_from_workflow(self, workflow: "Workflow"):

        steps = list(workflow._steps.values())
        inputs = dict(workflow.current_pipeline_inputs)
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
            "steps": [s.instance_cid for s in self.steps],
            "inputs": {k: str(v) for k, v in self.inputs.items()},
        }

    def set_inputs(self, **inputs: uuid.UUID):

        for k, v in inputs.items():
            if k in self.pipeline_config.structure.pipeline_inputs_schema.keys():
                self.inputs[k] = v

    @property
    def pipeline_config(self) -> PipelineConfig:

        return self.pipeline_info.pipeline_config

    @property
    def pipeline_structure(self) -> PipelineStructure:
        return self.pipeline_info.pipeline_config.structure

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


class WorkflowMetadata(KiaraModel):
    _kiara_model_id = "instance.workflow"

    workflow_id: uuid.UUID = Field(
        description="The globaly unique uuid for this workflow."
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
    workflow_history: Dict[datetime.datetime, str] = Field(
        description="A history of all the states of this workflow.",
        default_factory=dict,
    )

    input_aliases: Dict[str, str] = Field(
        description="A set of aliases that can be used to forward inputs to their (unaliased) pipeline inputs.",
        default_factory=dict,
    )
    output_aliases: Dict[str, str] = Field(
        description="A set of aliases to make output field names more user friendly.",
        default_factory=dict,
    )

    is_persisted: bool = Field(
        description="Whether this workflow is persisted in it's current state in a kiara store.",
        default=False,
    )

    _kiara: Union["Kiara", None] = PrivateAttr(default=None)
    # _last_update: datetime.datetime = PrivateAttr(default_factory=datetime.datetime.now)

    @validator("documentation", pre=True)
    def validate_doc(cls, value):
        if not isinstance(value, DocumentationMetadataModel):
            return DocumentationMetadataModel.create(value)
        else:
            return value

    @property
    def last_state_id(self) -> Union[None, str]:

        if not self.workflow_history:
            return None
        last_date = max(self.workflow_history.keys())
        workflow_state_id = self.workflow_history[last_date]
        return workflow_state_id


class WorkflowInfo(ItemInfo):

    _kiara_model_id = "info.workflow"

    @classmethod
    def create_from_workflow(cls, workflow: "Workflow"):

        wf_info = WorkflowInfo.construct(
            type_name=str(workflow.workflow_id),
            workflow_metadata=workflow.workflow_metadata,
            workflow_state_ids=workflow.all_state_ids,
            pipeline_info=workflow.pipeline_info,
            documentation=workflow.workflow_metadata.documentation,
            authors=workflow.workflow_metadata.authors,
            context=workflow.workflow_metadata.context,
            current_input_values=workflow.current_input_values,
            current_output_values=workflow.current_output_values,
            input_aliases=dict(workflow.input_aliases),
            output_aliases=dict(workflow.output_aliases),
        )
        return wf_info

    @classmethod
    def category_name(cls) -> str:
        return "workflow"

    @classmethod
    def base_instance_class(cls) -> Type["Workflow"]:
        from kiara.interfaces.python_api.workflow import Workflow

        return Workflow

    @classmethod
    def create_from_instance(cls, kiara: "Kiara", instance: "Workflow", **kwargs):

        return cls.create_from_workflow(workflow=instance)

    workflow_metadata: WorkflowMetadata = Field(description="The workflow details.")
    workflow_state_ids: List[str] = Field(description="All states for this workflow.")
    pipeline_info: PipelineInfo = Field(
        description="The current state of the workflows' pipeline."
    )
    current_input_values: ValueMap = Field(
        description="The current workflow inputs (after aliasing)."
    )
    current_output_values: ValueMap = Field(
        description="The current workflow outputs (after aliasing)."
    )
    input_aliases: Dict[str, str] = Field(
        description="The (current) input aliases for this workflow."
    )
    output_aliases: Dict[str, str] = Field(
        description="The (current) output aliases for this workflow."
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
        include_id = config.get("include_id", True)
        include_context = config.get("include_context", True)
        include_history = config.get("include_history", True)
        include_current_inputs = config.get("include_current_inputs", True)
        include_current_outputs = config.get("include_current_outputs", True)
        include_aliases = config.get("include_aliases", True)
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
        if include_id:
            table.add_row("workflow id", str(self.workflow_metadata.workflow_id))
        if include_context:
            table.add_row("context", self.context.create_renderable(**config))
        if include_aliases:
            aliases = orjson_dumps(
                {"inputs": self.input_aliases, "outputs": self.output_aliases}
            )
            table.add_row(
                "current aliases", Syntax(aliases, "json", background_color="default")
            )
        if include_current_inputs:
            inputs_renderable = self.current_input_values.create_renderable(**config)
            table.add_row("current inputs", inputs_renderable)
        if include_current_outputs:
            outputs_renderable = self.current_output_values.create_renderable(**config)
            table.add_row("current outputs", outputs_renderable)
        if include_history:
            history_table = Table(show_header=False, box=box.SIMPLE)
            history_table.add_column("date", style="i")
            history_table.add_column("id")
            for d, s_id in self.workflow_metadata.workflow_history.items():
                history_table.add_row(str(d), s_id)
            table.add_row("snapshot timeline", history_table)

        if include_current_state:
            current_state_id = (
                "-- n/a --"
                if not self.workflow_metadata.current_state
                else self.workflow_metadata.current_state
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
            steps = len(wf.pipeline_info.pipeline_config.structure.steps)
            stages = len(wf.pipeline_info.pipeline_config.structure.processing_stages)
            states = len(wf.workflow_state_ids)

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
