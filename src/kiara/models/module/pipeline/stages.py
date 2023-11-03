# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Set, Union

import networkx as nx
from pydantic import Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.defaults import KIARA_DEFAULT_STAGES_EXTRACTION_TYPE
from kiara.exceptions import KiaraException
from kiara.models import KiaraModel

if TYPE_CHECKING:
    from kiara.models.module.pipeline import PipelineStructure


class PipelineStage(KiaraModel):

    _kiara_model_id: ClassVar = "info.pipeline_stage"

    @classmethod
    def extract_stages(
        cls,
        structure: "PipelineStructure",
        stages_extraction_type: str = KIARA_DEFAULT_STAGES_EXTRACTION_TYPE,
    ) -> List[List[str]]:

        func_name = f"extract_stages__{stages_extraction_type}"
        if not hasattr(cls, func_name):
            msg = f"Invalid stages extraction type: {stages_extraction_type}."
            available = [x for x in dir(cls) if x.startswith("extract_stages__")]
            details = "Available stages extraction types:\n"
            for avail in available:
                details += f" - {avail.replace('extract_stages__', '')}\n"
            raise KiaraException(msg, details=details)

        result: List[List[str]] = getattr(cls, func_name)(structure=structure)
        return result

    @classmethod
    def extract_stages__single_stage(
        cls, structure: "PipelineStructure"
    ) -> List[List[str]]:
        """Extract a single stage from the pipeline structure.."""
        return [[step.step_id for step in structure.steps]]

    @classmethod
    def extract_stages__stage_per_step(
        cls, structure: "PipelineStructure"
    ) -> List[List[str]]:
        """Extract a stage for each step in the pipeline structure."""
        flat_list = [
            [item] for sublist in structure.processing_stages for item in sublist
        ]
        return flat_list

    @classmethod
    def extract_stages__late(cls, structure: "PipelineStructure") -> List[List[str]]:
        """Extract stages in a way so that steps are processed as late as possible."""

        execution_graph = structure.execution_graph
        leaf_nodes = [
            node
            for node in execution_graph.nodes()
            if execution_graph.in_degree(node) != 0
            and execution_graph.out_degree(node) == 0
        ]

        layers = {}
        for leaf_node in leaf_nodes:
            node_layers = nx.bfs_layers(execution_graph.reverse(), leaf_node)
            layers[leaf_node] = list(node_layers)

        stages: Dict[int, List[str]] = {}
        for step in structure.steps:
            step_id = step.step_id
            max_idx = 0
            for node_layers in layers.values():
                for idx, node_layer in enumerate(node_layers):
                    if step_id in node_layer:
                        if idx > max_idx:
                            max_idx = idx
                        break
            stages.setdefault(max_idx, []).append(step_id)

        processing_stages = []
        for stage_idx in sorted(stages.keys(), reverse=True):
            stage = stages[stage_idx]
            processing_stages.append(stage)

        return processing_stages

    @classmethod
    def extract_stages__early(cls, structure: "PipelineStructure") -> List[List[str]]:
        """Extract stages in a way so that steps are processed as early as possible."""
        execution_graph = structure.execution_graph
        processing_stages = []
        path_lengths: Dict[str, int] = {}
        for step in structure.steps:

            step_id = step.step_id

            paths = list(nx.all_simple_paths(execution_graph, "__root__", step_id))
            max_steps = max(paths, key=lambda x: len(x))
            path_lengths[step_id] = len(max_steps) - 1

        if path_lengths.values():
            max_length = max(path_lengths.values())

            for i in range(1, max_length + 1):
                stage: List[str] = [
                    m for m, length in path_lengths.items() if length == i
                ]
                processing_stages.append(stage)
                # for _step_id in stage:
                #     steps_details[_step_id]["processing_stage"] = i

        return processing_stages

    @classmethod
    def stages_info_from_pipeline_structure(
        cls,
        structure: "PipelineStructure",
        stages: Union[List[List[str]], str] = KIARA_DEFAULT_STAGES_EXTRACTION_TYPE,
    ) -> List["PipelineStage"]:

        if isinstance(stages, str):
            if stages == "late":
                stages = cls.extract_stages__late(structure=structure)
            elif stages == "early":
                stages = cls.extract_stages__early(structure=structure)
            else:
                raise Exception(
                    "Invalid value for 'stages': {stages!r} (must be 'late' or 'early'."
                )

        used_pipeline_inputs: Set[str] = set()
        used_pipeline_outputs: Set[str] = set()
        result = []
        for idx, stage in enumerate(stages, start=1):
            stage_steps = []
            pipeline_inputs = []
            pipeline_outputs = []
            connected_outputs = []
            stage_outputs = []

            for step_id in stage:
                step = structure.get_step(step_id=step_id)
                stage_steps.append(step.step_id)

                for pipeline_output, out_ref in structure.pipeline_output_refs.items():
                    if pipeline_output in used_pipeline_outputs:
                        continue
                    if out_ref.connected_output.step_id == step_id:
                        pipeline_outputs.append(pipeline_output)

                for field_name, input_ref in structure.get_step_input_refs(
                    step_id=step_id
                ).items():
                    if input_ref.connected_pipeline_input:
                        pipeline_inputs.append(input_ref.connected_pipeline_input)
                    elif input_ref.connected_outputs:
                        for con_out in input_ref.connected_outputs:
                            connected_outputs.append(con_out.alias)

                for field_name, output_ref in structure.get_step_output_refs(
                    step_id=step_id
                ).items():
                    if output_ref.pipeline_output:
                        pipeline_outputs.append(output_ref.pipeline_output)
                    if output_ref.connected_inputs:
                        stage_outputs.append(output_ref.alias)

            stage_used_pipeline_inputs = list(used_pipeline_inputs)
            stage_used_pipeline_outputs = list(used_pipeline_outputs)

            result.append(
                PipelineStage(
                    stage_index=idx,
                    steps=stage_steps,
                    connected_outputs=connected_outputs,
                    stage_outputs=stage_outputs,
                    pipeline_inputs=pipeline_inputs,
                    pipeline_outputs=pipeline_outputs,
                    previous_pipeline_inputs=stage_used_pipeline_inputs,
                    previous_pipeline_outputs=stage_used_pipeline_outputs,
                )
            )

            used_pipeline_inputs.update(pipeline_inputs)
            used_pipeline_outputs.update(pipeline_outputs)

        return result

    stage_index: int = Field(description="The index of this stage.")
    steps: List[str] = Field(
        description="The pipeline steps that are executed in this stage."
    )
    connected_outputs: List[str] = Field(
        description="Previous step outputs that are connected to this stage."
    )
    stage_outputs: List[str] = Field(description="The outputs of this stage.")
    pipeline_inputs: List[str] = Field(
        description="The pipeline inputs required for this stage."
    )
    pipeline_outputs: List[str] = Field(
        description="The pipeline outputs that are ready once this stage is processed."
    )
    previous_pipeline_inputs: List[str] = Field(
        description="Pipeline inputs that are already set by this stage."
    )
    previous_pipeline_outputs: List[str] = Field(
        description="Pipeline outputs that are already computed by this stage."
    )

    _graph: Union[None, nx.DiGraph] = PrivateAttr(default=None)

    def get_graph_fragment(self) -> nx.DiGraph:
        if self._graph is not None:
            return self._graph

        fragment = nx.DiGraph()
        stage_id = f"Stage: {self.stage_index}"
        fragment.add_node(stage_id, type="stage", stage_index=self.stage_index)

        for pi in self.pipeline_inputs:
            node_id = f"Input: {pi}"
            fragment.add_node(node_id, type="pipeline_input")
            fragment.add_edge(node_id, stage_id, type="pipeline_input")
        for co in self.connected_outputs:
            fragment.add_node(co, type="connected_output")
            fragment.add_edge(co, stage_id, type="connected_output")
        for so in self.stage_outputs:
            fragment.add_node(so, type="stage_output")
            fragment.add_edge(stage_id, so, type="stage_output")
        for po in self.pipeline_outputs:
            node_id = f"Output: {po}"
            fragment.add_node(node_id, type="pipeline_output")
            fragment.add_edge(stage_id, node_id, type="pipeline_output")

        self._graph = fragment
        return self._graph


class PipelineStages(KiaraModel):
    @classmethod
    def create(
        cls,
        structure: "PipelineStructure",
        stages_extraction_type: str = KIARA_DEFAULT_STAGES_EXTRACTION_TYPE,
    ) -> "PipelineStages":

        stages_info = structure.extract_processing_stages_info(
            stages_extraction_type=stages_extraction_type
        )

        result = cls(stages=stages_info)
        result._structure = structure
        return result

    stages: List[PipelineStage] = Field(description="The pipeline stages.")
    _structure: Union[None, "PipelineStructure"] = PrivateAttr(default=None)

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("Stage")
        table.add_column("Details")

        for stage in self.stages:
            row = [f"Stage {stage.stage_index}", stage.create_renderable(**config)]
            table.add_row(*row)

        return table
