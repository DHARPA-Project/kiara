# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import networkx as nx
from functools import lru_cache
from pydantic import Field, PrivateAttr, root_validator
from rich.console import RenderableType
from rich.tree import Tree
from typing import Any, Dict, Iterable, List, Mapping, Set, Union

from kiara.exceptions import InvalidPipelineConfig
from kiara.models import KiaraModel
from kiara.models.module.pipeline import PipelineConfig, PipelineStep
from kiara.models.module.pipeline.value_refs import (
    PipelineInputRef,
    PipelineOutputRef,
    StepInputRef,
    StepOutputRef,
    StepValueAddress,
    generate_step_alias,
)
from kiara.models.values.value_schema import ValueSchema


def generate_pipeline_endpoint_name(step_id: str, value_name: str):

    return f"{step_id}__{value_name}"


class StepInfo(KiaraModel):

    _kiara_model_id = "info.pipeline_step"

    step: PipelineStep = Field(description="The pipeline step object.")
    inputs: Dict[str, StepInputRef] = Field(
        description="Reference(s) to the fields that feed this steps inputs."
    )
    outputs: Dict[str, StepOutputRef] = Field(
        description="Reference(s) to the fields that are fed by this steps outputs."
    )
    required: bool = Field(
        description="Whether this step is always required or whether all his outputs feed into optional input fields."
    )
    processing_stage: int = Field(
        description="The index of the processing stage of this step."
    )

    @property
    def step_id(self) -> str:
        return self.step.step_id


class PipelineStage(KiaraModel):

    _kiara_model_id = "info.pipeline_stage"

    @classmethod
    def from_pipeline_structure(
        cls, structure: "PipelineStructure"
    ) -> Dict[int, "PipelineStage"]:
        used_pipeline_inputs: Set[str] = set()
        used_pipeline_outputs: Set[str] = set()
        result = {}
        for idx, stage in enumerate(structure.processing_stages, start=1):
            stage_steps = []
            inputs = []
            outputs = []

            for step_id in stage:
                step = structure.get_step(step_id=step_id)
                stage_steps.append(step.step_id)
                for pipeline_input, ref in structure.pipeline_input_refs.items():
                    if pipeline_input in used_pipeline_inputs:
                        continue
                    for con_inp in ref.connected_inputs:
                        if con_inp.step_id == step_id and pipeline_input not in inputs:
                            inputs.append(pipeline_input)
                for pipeline_output, out_ref in structure.pipeline_output_refs.items():
                    if pipeline_output in used_pipeline_outputs:
                        continue
                    if out_ref.connected_output.step_id == step_id:
                        outputs.append(pipeline_output)

            stage_used_inputs = list(used_pipeline_inputs)
            stage_used_outputs = list(used_pipeline_outputs)

            result[idx] = PipelineStage(
                stage_index=idx,
                steps=stage_steps,
                pipeline_inputs=inputs,
                pipeline_outputs=outputs,
                previous_inputs=stage_used_inputs,
                previous_outputs=stage_used_outputs,
            )

            used_pipeline_inputs.update(inputs)
            used_pipeline_outputs.update(outputs)

        return result

    stage_index: int = Field(description="The index of this stage.")
    steps: List[str] = Field(
        description="The pipeline steps that are executed in this stage."
    )
    pipeline_inputs: List[str] = Field(
        description="The pipeline inputs required for this stage."
    )
    pipeline_outputs: List[str] = Field(
        description="The pipeline outputs that are ready once this stage is processed."
    )
    previous_inputs: List[str] = Field(
        description="Pipeline inputs that are already set by this stage."
    )
    previous_outputs: List[str] = Field(
        description="Pipeline outputs that are already computed by this stage."
    )


class PipelineStructure(KiaraModel):
    """An object that holds one or several steps, and describes the connections between them."""

    _kiara_model_id = "instance.pipeline_structure"

    pipeline_config: PipelineConfig = Field(
        description="The underlying pipeline config."
    )
    steps: List[PipelineStep] = Field(description="The pipeline steps ")
    # stages: Mapping[int, PipelineStage] = Field(description="Details about each of the pipeline stages.")
    input_aliases: Dict[str, str] = Field(description="The input aliases.")
    output_aliases: Dict[str, str] = Field(description="The output aliases.")

    @root_validator(pre=True)
    def validate_pipeline_config(cls, values) -> Dict[str, Any]:

        pipeline_config = values.get("pipeline_config", None)
        if not pipeline_config:
            raise ValueError("No 'pipeline_config' provided.")

        if len(values) != 1:
            raise ValueError(
                "Only 'pipeline_config' key allowed when creating a pipeline structure object."
            )

        if isinstance(pipeline_config, Mapping):
            pipeline_config = PipelineConfig(**pipeline_config)
        _config: PipelineConfig = pipeline_config
        _steps: List[PipelineStep] = list(_config.steps)

        _input_aliases: Dict[str, str] = dict(_config.input_aliases)
        _output_aliases: Dict[str, str] = dict(_config.output_aliases)

        invalid_input_aliases = [a for a in _input_aliases.values() if "." in a]
        if invalid_input_aliases:
            raise InvalidPipelineConfig(
                f"Invalid input aliases, aliases can't contain special characters: {', '.join(invalid_input_aliases)}.",
                config=values.get("pipeline_config", None),
                details=f"Invalid characters: {', '.join(invalid_input_aliases)}.",
            )
        invalid_output_aliases = [a for a in _input_aliases.values() if "." in a]
        if invalid_input_aliases:
            raise InvalidPipelineConfig(
                f"Invalid input aliases, aliases can't contain special characters: {', '.join(invalid_output_aliases)}.",
                config=values.get("pipeline_config"),
                details=f"Invalid characters: {', '.join(invalid_output_aliases)}.",
            )

        valid_input_names = set()
        for step in _steps:
            for input_name in step.module.input_names:
                valid_input_names.add(f"{step.step_id}.{input_name}")
        invalid_input_aliases = [
            a for a in _input_aliases.keys() if a not in valid_input_names
        ]
        if invalid_input_aliases:
            msg = "Invalid input reference(s)."
            details = "Invalid reference(s):\n"
            for iia in invalid_input_aliases:
                details += f" - {iia}\n"
            details += "\nMust be one of: \n"
            for name in valid_input_names:
                details += f"  - {name}\n"

            raise InvalidPipelineConfig(
                msg, config=values.get("pipeline_config", {}), details=details
            )

        valid_output_names = set()
        for step in _steps:
            for output_name in step.module.output_names:
                valid_output_names.add(f"{step.step_id}.{output_name}")
        invalid_output_names = [
            a for a in _output_aliases.keys() if a not in valid_output_names
        ]
        if invalid_output_names:

            msg = "Invalid output reference(s)."
            details = "Invalid reference(s):\n"
            for iia in invalid_output_names:
                details += f" - {iia}\n"
            details += "\nMust be one of: \n"
            for name in valid_output_names:
                details += f"  - {name}\n"

            raise InvalidPipelineConfig(msg, values.get("pipeline_config", {}), details)

        # stages = PipelineStage.from_pipeline_structure(stages=)

        # values["steps"] = {step.step_id: step for step in _steps}
        values["steps"] = _steps
        values["input_aliases"] = _input_aliases
        values["output_aliases"] = _output_aliases
        return values

    # this is hardcoded for now
    _add_all_workflow_outputs: bool = PrivateAttr(default=False)
    _constants: Dict[str, Any] = PrivateAttr(default=None)  # type: ignore
    _defaults: Dict[str, Any] = PrivateAttr(None)  # type: ignore

    _execution_graph: nx.DiGraph = PrivateAttr(None)  # type: ignore
    _data_flow_graph: nx.DiGraph = PrivateAttr(None)  # type: ignore
    _data_flow_graph_simple: nx.DiGraph = PrivateAttr(None)  # type: ignore

    _processing_stages: List[List[str]] = PrivateAttr(None)  # type: ignore
    _stages_info: Mapping[int, PipelineStage] = PrivateAttr(None)  # type: ignore

    # holds details about the (current) processing steps contained in this workflow
    _steps_details: Dict[str, StepInfo] = PrivateAttr(None)  # type: ignore
    # _info: "PipelineStructureInfo" = PrivateAttr(None)  # type: ignore

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "steps": [step.instance_cid for step in self.steps],
            "input_aliases": self.input_aliases,
            "output_aliases": self.output_aliases,
        }

    def _retrieve_id(self) -> str:
        return self.pipeline_config.instance_id

    @property
    def steps_details(self) -> Mapping[str, StepInfo]:

        if self._steps_details is None:
            self._process_steps()
        return self._steps_details  # type: ignore

    @property
    def step_ids(self) -> Iterable[str]:
        if self._steps_details is None:
            self._process_steps()
        return self._steps_details.keys()  # type: ignore

    @property
    def constants(self) -> Mapping[str, Any]:

        if self._constants is None:
            self._process_steps()
        return self._constants  # type: ignore

    @property
    def defaults(self) -> Mapping[str, Any]:

        if self._defaults is None:
            self._process_steps()
        return self._defaults  # type: ignore

    def get_step(self, step_id: str) -> PipelineStep:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No step with id: {step_id}")

        return d.step

    def get_step_input_refs(self, step_id: str) -> Mapping[str, StepInputRef]:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No step with id: {step_id}")

        return d.inputs

    def get_step_output_refs(self, step_id: str) -> Mapping[str, StepOutputRef]:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No step with id: {step_id}")

        return d.outputs

    def get_step_details(self, step_id: str) -> StepInfo:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No step with id: {step_id}")

        return d

    @property
    def execution_graph(self) -> nx.DiGraph:
        if self._execution_graph is None:
            self._process_steps()
        return self._execution_graph

    @property
    def data_flow_graph(self) -> nx.DiGraph:
        if self._data_flow_graph is None:
            self._process_steps()
        return self._data_flow_graph

    @property
    def data_flow_graph_simple(self) -> nx.DiGraph:
        if self._data_flow_graph_simple is None:
            self._process_steps()
        return self._data_flow_graph_simple

    @property
    def processing_stages(self) -> List[List[str]]:
        if self._steps_details is None:
            self._process_steps()
        return self._processing_stages

    @property
    def processing_stages_info(self) -> Mapping[int, PipelineStage]:
        if self._stages_info is not None:
            return self._stages_info

        self._stages_info = PipelineStage.from_pipeline_structure(self)
        return self._stages_info

    @lru_cache()
    def _get_node_of_type(self, node_type: str):
        if self._steps_details is None:
            self._process_steps()

        return [
            node
            for node, attr in self._data_flow_graph.nodes(data=True)
            if attr["type"] == node_type
        ]

    @property
    def steps_input_refs(self) -> Dict[str, StepInputRef]:
        return {
            node.alias: node
            for node in self._get_node_of_type(node_type=StepInputRef.__name__)
        }

    @property
    def steps_output_refs(self) -> Dict[str, StepOutputRef]:
        return {
            node.alias: node
            for node in self._get_node_of_type(node_type=StepOutputRef.__name__)
        }

    @property
    def pipeline_input_refs(self) -> Dict[str, PipelineInputRef]:
        return {
            node.value_name: node
            for node in self._get_node_of_type(node_type=PipelineInputRef.__name__)
        }

    @property
    def pipeline_output_refs(self) -> Dict[str, PipelineOutputRef]:
        return {
            node.value_name: node
            for node in self._get_node_of_type(node_type=PipelineOutputRef.__name__)
        }

    @property
    def pipeline_inputs_schema(self) -> Mapping[str, ValueSchema]:

        schemas = {
            input_name: w_in.value_schema
            for input_name, w_in in self.pipeline_input_refs.items()
        }
        return schemas

    @property
    def pipeline_outputs_schema(self) -> Mapping[str, ValueSchema]:
        return {
            output_name: w_out.value_schema
            for output_name, w_out in self.pipeline_output_refs.items()
        }

    def get_pipeline_inputs_schema_for_step(
        self, step_id: str
    ) -> Mapping[str, ValueSchema]:

        result = {}
        for field, ref in self.pipeline_input_refs.items():
            for con in ref.connected_inputs:
                if con.step_id == step_id:
                    result[field] = ref.value_schema
                    break
        return result

    def get_pipeline_outputs_schema_for_step(
        self, step_id: str
    ) -> Mapping[str, ValueSchema]:

        result = {}
        for field, ref in self.pipeline_output_refs.items():
            if ref.connected_output.step_id == step_id:
                result[field] = ref.value_schema

        return result

    def get_processing_stage(self, step_id: str) -> int:
        """Return the processing stage for the specified step_id.

        Returns the stage nr (starting with '1').
        """

        for index, stage in enumerate(self.processing_stages, start=1):
            if step_id in stage:
                return index

        raise Exception(f"Invalid step id '{step_id}'.")

    def step_is_required(self, step_id: str) -> bool:
        """Check if the specified step is required, or can be omitted."""

        return self.get_step_details(step_id=step_id).required

    def _process_steps(self) -> None:
        """The core method of this class, it connects all the processing modules, their inputs and outputs."""

        steps_details: Dict[str, Any] = {}
        execution_graph = nx.DiGraph()
        execution_graph.add_node("__root__")
        data_flow_graph = nx.DiGraph()
        data_flow_graph_simple = nx.DiGraph()
        processing_stages = []
        constants = {}
        structure_defaults = {}

        # temp variable, to hold all outputs
        outputs: Dict[str, StepOutputRef] = {}

        # process all pipeline and step outputs first
        _temp_steps_map: Dict[str, PipelineStep] = {}
        pipeline_outputs: Dict[str, PipelineOutputRef] = {}
        for step in self.steps:

            _temp_steps_map[step.step_id] = step

            if step.step_id in steps_details.keys():
                raise Exception(
                    f"Can't process steps: duplicate step_id '{step.step_id}'"
                )

            steps_details[step.step_id] = {
                "step": step,
                "outputs": {},
                "inputs": {},
                "required": True,
            }

            data_flow_graph.add_node(step, type="step")
            data_flow_graph_simple.add_node(step, type="step")

            # go through all the module outputs, create points for them and connect them to pipeline outputs
            for output_name, schema in step.module.outputs_schema.items():

                step_output = StepOutputRef(
                    value_name=output_name,
                    value_schema=schema,
                    step_id=step.step_id,
                    pipeline_output=None,
                )

                steps_details[step.step_id]["outputs"][output_name] = step_output
                step_alias = generate_step_alias(step.step_id, output_name)
                outputs[step_alias] = step_output

                # step_output_name = generate_pipeline_endpoint_name(
                #     step_id=step.step_id, value_name=output_name
                # )
                step_output_name: Union[None, str] = f"{step.step_id}.{output_name}"
                if not self.output_aliases:
                    raise NotImplementedError()
                if step_output_name in self.output_aliases.keys():
                    step_output_name = self.output_aliases[step_output_name]  # type: ignore
                else:
                    if not self._add_all_workflow_outputs:
                        # this output is not interesting for the workflow
                        step_output_name = None

                if step_output_name:
                    step_output_address = StepValueAddress(
                        step_id=step.step_id, value_name=output_name
                    )
                    pipeline_output = PipelineOutputRef(
                        value_name=step_output_name,
                        connected_output=step_output_address,
                        value_schema=schema,
                    )
                    pipeline_outputs[step_output_name] = pipeline_output
                    step_output.pipeline_output = pipeline_output.value_name

                    data_flow_graph.add_node(
                        pipeline_output, type=PipelineOutputRef.__name__
                    )
                    data_flow_graph.add_edge(step_output, pipeline_output)

                    data_flow_graph_simple.add_node(
                        pipeline_output, type=PipelineOutputRef.__name__
                    )
                    data_flow_graph_simple.add_edge(step, pipeline_output)

                data_flow_graph.add_node(step_output, type=StepOutputRef.__name__)
                data_flow_graph.add_edge(step, step_output)

        # now process inputs, and connect them to the appropriate output/pipeline-input points
        existing_pipeline_input_points: Dict[str, PipelineInputRef] = {}
        for step in self.steps:

            other_step_dependency: Set = set()
            # go through all the inputs of a module, create input points and connect them to either
            # other module outputs, or pipeline inputs (which need to be created)

            module_constants: Mapping[str, Any] = step.module.get_config_value(
                "constants"
            )

            for input_name, schema in step.module.inputs_schema.items():

                matching_input_links: List[StepValueAddress] = []
                is_constant = input_name in module_constants.keys()

                for value_name, input_links in step.input_links.items():
                    if value_name == input_name:
                        for input_link in input_links:
                            if input_link in matching_input_links:
                                raise Exception(f"Duplicate input link: {input_link}")
                            matching_input_links.append(input_link)

                if matching_input_links:
                    # this means we connect to other steps output

                    connected_output_points: List[StepOutputRef] = []
                    connected_outputs: List[StepValueAddress] = []

                    for input_link in matching_input_links:
                        output_id = generate_step_alias(
                            input_link.step_id, input_link.value_name
                        )

                        if output_id not in outputs.keys():
                            raise Exception(
                                f"Can't connect input '{input_name}' for step '{step.step_id}': no output '{output_id}' available. Available output names: {', '.join(outputs.keys())}"
                            )
                        connected_output_points.append(outputs[output_id])
                        connected_outputs.append(input_link)

                        other_step_dependency.add(input_link.step_id)

                    step_input_point = StepInputRef(
                        step_id=step.step_id,
                        value_name=input_name,
                        value_schema=schema,
                        is_constant=is_constant,
                        connected_pipeline_input=None,
                        connected_outputs=connected_outputs,
                    )

                    for op in connected_output_points:
                        op.connected_inputs.append(step_input_point.address)
                        data_flow_graph.add_edge(op, step_input_point)
                        data_flow_graph_simple.add_edge(
                            _temp_steps_map[op.step_id], step_input_point
                        )  # TODO: name edge
                        data_flow_graph_simple.add_edge(
                            step_input_point, step
                        )  # TODO: name edge

                else:
                    # this means we connect to pipeline input
                    # pipeline_input_name = generate_pipeline_endpoint_name(
                    #     step_id=step.step_id, value_name=input_name
                    # )
                    pipeline_input_ref = f"{step.step_id}.{input_name}"

                    # check whether this input has an alias associated with it
                    if not self.input_aliases:
                        raise NotImplementedError()

                    if pipeline_input_ref in self.input_aliases.keys():
                        # this means we use the pipeline alias
                        pipeline_input_name = self.input_aliases[pipeline_input_ref]
                    else:
                        pipeline_input_name = generate_pipeline_endpoint_name(
                            step_id=step.step_id, value_name=input_name
                        )

                    if pipeline_input_name in existing_pipeline_input_points.keys():
                        # we already created a pipeline input with this name
                        # TODO: check whether schema fits
                        connected_pipeline_input = existing_pipeline_input_points[
                            pipeline_input_name
                        ]
                        assert connected_pipeline_input.is_constant == is_constant
                    else:
                        # we need to create the pipeline input
                        connected_pipeline_input = PipelineInputRef(
                            value_name=pipeline_input_name,
                            value_schema=schema,
                            is_constant=is_constant,
                        )

                        existing_pipeline_input_points[
                            pipeline_input_name
                        ] = connected_pipeline_input

                        data_flow_graph.add_node(
                            connected_pipeline_input, type=PipelineInputRef.__name__
                        )
                        data_flow_graph_simple.add_node(
                            connected_pipeline_input, type=PipelineInputRef.__name__
                        )
                        if is_constant:
                            constants[
                                pipeline_input_name
                            ] = step.module.get_config_value("constants")[input_name]

                        default_val = step.module.get_config_value("defaults").get(
                            input_name, None
                        )
                        if is_constant and default_val is not None:
                            raise Exception(
                                f"Module config invalid for step '{step.step_id}': both default value and constant provided for input '{input_name}'."
                            )
                        elif default_val is not None:
                            structure_defaults[pipeline_input_name] = default_val

                    step_input_point = StepInputRef(
                        step_id=step.step_id,
                        value_name=input_name,
                        value_schema=schema,
                        connected_pipeline_input=connected_pipeline_input.value_name,
                        connected_outputs=None,
                        is_constant=is_constant,
                    )
                    connected_pipeline_input.connected_inputs.append(
                        step_input_point.address
                    )
                    data_flow_graph.add_edge(connected_pipeline_input, step_input_point)
                    data_flow_graph_simple.add_edge(connected_pipeline_input, step)

                data_flow_graph.add_node(step_input_point, type=StepInputRef.__name__)

                steps_details[step.step_id]["inputs"][input_name] = step_input_point

                data_flow_graph.add_edge(step_input_point, step)

            if other_step_dependency:
                for module_id in other_step_dependency:
                    execution_graph.add_edge(module_id, step.step_id)
            else:
                execution_graph.add_edge("__root__", step.step_id)

        # calculate execution order
        path_lengths: Dict[str, int] = {}

        for step in self.steps:

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
                for _step_id in stage:
                    steps_details[_step_id]["processing_stage"] = i
                    # steps_details[_step_id]["step"].processing_stage = i

        self._constants = constants
        self._defaults = structure_defaults
        self._steps_details = {
            step_id: StepInfo(**data) for step_id, data in steps_details.items()
        }
        self._execution_graph = execution_graph
        self._data_flow_graph = data_flow_graph
        self._data_flow_graph_simple = data_flow_graph_simple
        self._processing_stages = processing_stages

        self._get_node_of_type.cache_clear()

    # def info(self) -> "PipelineStructureInfo":
    #
    #     if self._info is not None:
    #         return self._info
    #
    #     from kiara.interfaces.python_api.models.info import PipelineStructureInfo
    #
    #     self._info = PipelineStructureInfo.create_from_instance(kiara=None, instance=self)  # type: ignore
    #     return self._info

    def create_renderable(self, **config: Any) -> RenderableType:

        tree = Tree("pipeline")
        inputs = tree.add("inputs")
        for field_name, schema in self.pipeline_inputs_schema.items():
            inputs.add(f"[i]{field_name}[i] (type: {schema.type})")

        steps = tree.add("steps")
        for idx, stage in enumerate(self.processing_stages, start=1):
            stage_node = steps.add(f"stage {idx}")
            for step_id in stage:
                step_node = stage_node.add(f"step: {step_id}")
                step = self.get_step(step_id=step_id)
                if step.doc.is_set:
                    step_node.add(f"desc: {step.doc.description}")
                step_node.add(f"module: {step.manifest_src.module_type}")

        outputs = tree.add("outputs")
        for field_name, schema in self.pipeline_outputs_schema.items():
            outputs.add(f"[i]{field_name}[i] (type: {schema.type})")

        return tree
