# -*- coding: utf-8 -*-
import networkx as nx
import typing
from deepdiff import DeepHash
from functools import lru_cache
from pydantic import BaseModel, Extra, Field, PrivateAttr

from kiara.data.values import (
    PipelineInputField,
    PipelineOutputField,
    StepInputField,
    StepOutputField,
    StepValueAddress,
    ValueSchema,
    generate_step_alias,
)
from kiara.defaults import PIPELINE_PARENT_MARKER
from kiara.module import KiaraModule

if typing.TYPE_CHECKING:
    from kiara.config import PipelineStepConfig
    from kiara.kiara import Kiara


class PipelineStep(BaseModel):
    """A step within a pipeline-structure, includes information about it's connection(s) and other metadata."""

    class Config:
        validate_assignment = True
        extra = Extra.forbid

    @classmethod
    def create_steps(
        cls, parent_id: str, *steps: "PipelineStepConfig", kiara: "Kiara"
    ) -> typing.List["PipelineStep"]:

        result: typing.List[PipelineStep] = []
        if kiara is None:
            from kiara.module import Kiara

            kiara = Kiara.instance()

        for step in steps:

            _s = PipelineStep(
                step_id=step.step_id,
                parent_id=parent_id,
                module_type=step.module_type,
                module_config=step.module_config,
                input_links=step.input_links,
                _kiara=kiara,
            )
            result.append(_s)

        return result

    _module: typing.Optional[KiaraModule] = PrivateAttr(default=None)

    step_id: str
    parent_id: str
    module_type: str = Field(description="The module type.")
    module_config: typing.Mapping[str, typing.Any] = Field(
        description="The module config.", default_factory=dict
    )
    processing_stage: typing.Optional[int] = Field(
        default=None,
        description="The stage number this step is executed within the pipeline.",
    )
    input_links: typing.Mapping[str, typing.List[StepValueAddress]] = Field(
        description="The links that connect to inputs of the module.",
        default_factory=list,
    )
    _kiara: typing.Optional["Kiara"] = PrivateAttr(default=None)

    def __init__(self, **data):  # type: ignore
        kiara = data.pop("_kiara", None)
        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()
        super().__init__(**data)
        self._kiara: Kiara = kiara

    @property
    def kiara(self):
        return self._kiara

    @property
    def module(self) -> KiaraModule:

        if self._module is None:

            self._module = self.kiara.create_module(
                id=self.step_id,
                module_type=self.module_type,
                module_config=self.module_config,
            )
        return self._module

    def __eq__(self, other):

        if not isinstance(other, PipelineStep):
            return False

        # TODO: also check whether _kiara obj is equal?
        eq = (self.step_id, self.parent_id, self.module, self.processing_stage,) == (
            other.step_id,
            other.parent_id,
            other.module,
            other.processing_stage,
        )

        if not eq:
            return False

        hs = DeepHash(self.input_links)
        ho = DeepHash(other.input_links)

        return hs[self.input_links] == ho[other.input_links]

    def __hash__(self):

        # TODO: also include _kiara obj?
        # TODO: figure out whether that can be made to work without deephash
        hs = DeepHash(self.input_links)
        return hash(
            (
                self.step_id,
                self.parent_id,
                self.module,
                self.processing_stage,
                hs[self.input_links],
            )
        )

    def __repr__(self):

        return f"{self.__class__.__name__}(step_id={self.step_id} parent={self.parent_id} module_type={self.module_type} processing_stage={self.processing_stage}"

    def __str__(self):
        return self.__repr__()


def generate_pipeline_endpoint_name(step_id: str, value_name: str):

    return f"{step_id}__{value_name}"


class PipelineStructure(object):
    """An object that holds one or several steps, and describes the connections between them."""

    def __init__(
        self,
        parent_id: str,
        steps: typing.Iterable["PipelineStepConfig"],
        input_aliases: typing.Mapping[str, str] = None,
        output_aliases: typing.Mapping[str, str] = None,
        add_all_workflow_outputs: bool = False,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if not steps:
            raise Exception("No steps provided.")

        if kiara is None:
            kiara = Kiara.instance()

        self._steps: typing.List[PipelineStep] = PipelineStep.create_steps(
            parent_id, *steps, kiara=kiara
        )
        self._pipeline_id: str = parent_id

        if input_aliases is None:
            input_aliases = {}
        self._input_aliases: typing.Mapping[str, str] = input_aliases
        if output_aliases is None:
            output_aliases = {}
        self._output_aliases: typing.Mapping[str, str] = output_aliases

        self._add_all_workflow_outputs: bool = add_all_workflow_outputs

        self._execution_graph: nx.DiGraph = None  # type: ignore
        self._data_flow_graph: nx.DiGraph = None  # type: ignore
        self._data_flow_graph_simple: nx.DiGraph = None  # type: ignore

        self._processing_stages: typing.List[typing.List[str]] = None  # type: ignore

        self._steps_details: typing.Dict[str, typing.Any] = None  # type: ignore
        """Holds details about the (current) processing steps contained in this workflow."""

    @property
    def pipeline_id(self) -> str:
        return self._pipeline_id

    @property
    def steps(self) -> typing.Iterable[PipelineStep]:
        return self._steps

    @property
    def modules(self) -> typing.Iterable[KiaraModule]:
        return (s.module for s in self.steps)

    @property
    def steps_details(self) -> typing.Mapping[str, typing.Any]:

        if self._steps_details is None:
            self._process_steps()
        return self._steps_details

    def get_step(self, step_id: str) -> PipelineStep:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No module with id: {step_id}")

        return d["step"]

    def get_step_inputs(self, step_id: str) -> typing.Iterable[StepInputField]:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No module with id: {step_id}")

        return d["inputs"]

    def get_step_outputs(self, step_id: str) -> typing.Iterable[StepOutputField]:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No module with id: {step_id}")

        return d["outputs"]

    def get_step_details(self, step_id: str) -> typing.Mapping[str, typing.Any]:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No module with id: {step_id}")

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
    def processing_stages(self) -> typing.List[typing.List[str]]:
        if self._steps_details is None:
            self._process_steps()
        return self._processing_stages

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
    def steps_inputs(self) -> typing.Dict[str, StepInputField]:
        return {
            node.alias: node
            for node in self._get_node_of_type(node_type=StepInputField.__name__)
        }

    @property
    def steps_outputs(self) -> typing.Dict[str, StepOutputField]:
        return {
            node.alias: node
            for node in self._get_node_of_type(node_type=StepOutputField.__name__)
        }

    @property
    def pipeline_inputs(self) -> typing.Dict[str, PipelineInputField]:
        return {
            node.value_name: node
            for node in self._get_node_of_type(node_type=PipelineInputField.__name__)
        }

    @property
    def pipeline_outputs(self) -> typing.Dict[str, PipelineOutputField]:
        return {
            node.value_name: node
            for node in self._get_node_of_type(node_type=PipelineOutputField.__name__)
        }

    @property
    def pipeline_input_schema(self) -> typing.Mapping[str, ValueSchema]:

        return {
            input_name: w_in.value_schema
            for input_name, w_in in self.pipeline_inputs.items()
        }

    @property
    def pipeline_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        return {
            output_name: w_out.value_schema
            for output_name, w_out in self.pipeline_outputs.items()
        }

    def _process_steps(self):
        """The core method of this class, it connects all the processing modules, their inputs and outputs."""

        steps_details: typing.Dict[str, typing.Any] = {}
        execution_graph = nx.DiGraph()
        execution_graph.add_node("__root__")
        data_flow_graph = nx.DiGraph()
        data_flow_graph_simple = nx.DiGraph()
        processing_stages = []

        # temp variable, to hold all outputs
        outputs: typing.Dict[str, StepOutputField] = {}

        # process all pipeline and step outputs first
        _temp_steps_map: typing.Dict[str, PipelineStep] = {}
        for step in self._steps:

            _temp_steps_map[step.step_id] = step

            if step.step_id in steps_details.keys():
                raise Exception(
                    f"Can't process steps: duplicate step_id '{step.step_id}'"
                )

            steps_details[step.step_id] = {
                "step": step,
                "outputs": {},
                "inputs": {},
            }

            data_flow_graph.add_node(step, type="step")

            # go through all the module outputs, create points for them and connect them to pipeline outputs
            for output_name, schema in step.module.output_schemas.items():

                step_output = StepOutputField(
                    value_name=output_name,
                    value_schema=schema,
                    step_id=step.step_id,
                    pipeline_id=self._pipeline_id,
                )

                steps_details[step.step_id]["outputs"][output_name] = step_output
                step_alias = generate_step_alias(step.step_id, output_name)
                outputs[step_alias] = step_output

                step_output_name = generate_pipeline_endpoint_name(
                    step_id=step.step_id, value_name=output_name
                )
                if self._output_aliases:
                    if step_output_name in self._output_aliases.keys():
                        step_output_name = self._output_aliases[step_output_name]
                    else:
                        if not self._add_all_workflow_outputs:
                            # this output is not interesting for the workflow
                            step_output_name = None

                if step_output_name:
                    step_output_address = StepValueAddress(
                        step_id=step.step_id, value_name=output_name
                    )
                    pipeline_output = PipelineOutputField(
                        pipeline_id=self._pipeline_id,
                        value_name=step_output_name,
                        connected_output=step_output_address,
                        value_schema=schema,
                    )
                    step_output.pipeline_output = pipeline_output.value_name

                    data_flow_graph.add_node(
                        pipeline_output, type=PipelineOutputField.__name__
                    )
                    data_flow_graph.add_edge(step_output, pipeline_output)

                    data_flow_graph_simple.add_node(
                        pipeline_output, type=PipelineOutputField.__name__
                    )
                    data_flow_graph_simple.add_edge(step, pipeline_output)

                data_flow_graph.add_node(step_output, type=StepOutputField.__name__)
                data_flow_graph.add_edge(step, step_output)

        # now process inputs, and connect them to the appropriate output/pipeline-input points
        existing_pipeline_input_points: typing.Dict[str, PipelineInputField] = {}
        for step in self._steps:

            other_step_dependency: typing.Set = set()
            # go through all the inputs of a module, create input points and connect them to either
            # other module outputs, or pipeline inputs (which need to be created)
            for input_name, schema in step.module.input_schemas.items():

                matching_input_links: typing.List[StepValueAddress] = []
                for value_name, input_links in step.input_links.items():
                    if value_name == input_name:
                        for input_link in input_links:
                            if input_link in matching_input_links:
                                raise Exception(f"Duplicate input link: {input_link}")
                            matching_input_links.append(input_link)

                if matching_input_links:
                    # this means we connect to other steps output

                    connected_output_points: typing.List[StepOutputField] = []
                    connected_outputs: typing.List[StepValueAddress] = []

                    for input_link in matching_input_links:
                        output_id = generate_step_alias(
                            input_link.step_id, input_link.value_name
                        )

                        if output_id not in outputs.keys():
                            raise Exception(
                                f"Can't connect input '{input_name}' for step '{step.step_id}': no output '{output_id}' available."
                            )
                        connected_output_points.append(outputs[output_id])
                        connected_outputs.append(input_link)

                        other_step_dependency.add(input_link.step_id)

                    step_input_point = StepInputField(
                        step_id=step.step_id,
                        pipeline_id=self._pipeline_id,
                        value_name=input_name,
                        value_schema=schema,
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
                    pipeline_input_name = generate_pipeline_endpoint_name(
                        step_id=step.step_id, value_name=input_name
                    )
                    if self._input_aliases:
                        if pipeline_input_name in self._input_aliases.keys():
                            # this means we use the pipeline alias
                            pipeline_input_name = self._input_aliases[
                                pipeline_input_name
                            ]

                    if pipeline_input_name in existing_pipeline_input_points.keys():
                        # we already created a pipeline input with this name
                        # TODO: check whether schema fits
                        connected_pipeline_input = existing_pipeline_input_points[
                            pipeline_input_name
                        ]
                    else:
                        # we need to create the pipeline input
                        connected_pipeline_input = PipelineInputField(
                            value_name=pipeline_input_name,
                            value_schema=schema,
                            pipeline_id=self._pipeline_id,
                        )

                        existing_pipeline_input_points[
                            pipeline_input_name
                        ] = connected_pipeline_input

                        data_flow_graph.add_node(
                            connected_pipeline_input, type=PipelineInputField.__name__
                        )
                        data_flow_graph_simple.add_node(
                            connected_pipeline_input, type=PipelineInputField.__name__
                        )

                    step_input_point = StepInputField(
                        step_id=step.step_id,
                        pipeline_id=self._pipeline_id,
                        value_name=input_name,
                        value_schema=schema,
                        connected_pipeline_input=connected_pipeline_input.value_name,
                        connected_outputs=None,
                    )
                    connected_pipeline_input.connected_inputs.append(
                        step_input_point.address
                    )
                    data_flow_graph.add_edge(connected_pipeline_input, step_input_point)
                    data_flow_graph_simple.add_edge(connected_pipeline_input, step)

                data_flow_graph.add_node(step_input_point, type=StepInputField.__name__)

                steps_details[step.step_id]["inputs"][input_name] = step_input_point

                data_flow_graph.add_edge(step_input_point, step)

            if other_step_dependency:
                for module_id in other_step_dependency:
                    execution_graph.add_edge(module_id, step.step_id)
            else:
                execution_graph.add_edge("__root__", step.step_id)

        # calculate execution order
        path_lengths: typing.Dict[str, int] = {}

        for step in self._steps:

            step_id = step.step_id

            paths = list(nx.all_simple_paths(execution_graph, "__root__", step_id))
            max_steps = max(paths, key=lambda x: len(x))
            path_lengths[step_id] = len(max_steps) - 1

        max_length = max(path_lengths.values())

        for i in range(1, max_length + 1):
            stage: typing.List[str] = [
                m for m, length in path_lengths.items() if length == i
            ]
            processing_stages.append(stage)
            for _step_id in stage:
                steps_details[_step_id]["processing_stage"] = i
                steps_details[_step_id]["step"].processing_stage = i

        self._steps_details = steps_details
        self._execution_graph = execution_graph
        self._data_flow_graph = data_flow_graph
        self._data_flow_graph_simple = data_flow_graph_simple
        self._processing_stages = processing_stages

        self._get_node_of_type.cache_clear()

    def to_details(self) -> "PipelineStructureDesc":

        steps = {}
        workflow_inputs: typing.Dict[str, typing.List[str]] = {}
        workflow_outputs: typing.Dict[str, str] = {}

        for m_id, details in self.steps_details.items():

            step = details["step"]

            input_connections: typing.Dict[str, typing.List[str]] = {}
            for k, v in details["inputs"].items():

                if v.connected_pipeline_input is not None:
                    connected_item = v.connected_pipeline_input
                    input_connections[k] = [
                        generate_step_alias(PIPELINE_PARENT_MARKER, connected_item)
                    ]
                    workflow_inputs.setdefault(f"{connected_item}", []).append(v.alias)
                elif v.connected_outputs is not None:
                    assert len(v.connected_outputs) > 0
                    for co in v.connected_outputs:
                        input_connections.setdefault(k, []).append(co.alias)
                else:
                    raise TypeError(f"Invalid connection type: {type(connected_item)}")

            output_connections: typing.Dict[str, typing.Any] = {}
            for k, v in details["outputs"].items():
                for connected_item in v.connected_inputs:

                    output_connections.setdefault(k, []).append(
                        generate_step_alias(
                            connected_item.step_id, connected_item.value_name
                        )
                    )
                if v.pipeline_output:
                    output_connections.setdefault(k, []).append(
                        generate_step_alias(PIPELINE_PARENT_MARKER, v.pipeline_output)
                    )
                    workflow_outputs[v.pipeline_output] = v.alias

            steps[step.step_id] = StepDesc(
                step=step,
                processing_stage=details["processing_stage"],
                input_connections=input_connections,
                output_connections=output_connections,
            )

        return PipelineStructureDesc(
            pipeline_id=self._pipeline_id,
            steps=steps,
            processing_stages=self.processing_stages,
            pipeline_input_connections=workflow_inputs,
            pipeline_output_connections=workflow_outputs,
        )


class StepDesc(BaseModel):
    """Details of a single [PipelineStep][kiara.pipeline.structure.PipelineStep] (which lives within a [Pipeline][kiara.pipeline.pipeline.Pipeline]"""

    class Config:
        allow_mutation = False
        extra = Extra.forbid

    step: PipelineStep = Field(description="Attributes of the step itself.")
    processing_stage: int = Field(
        description="The processing stage of this step within a Pipeline."
    )
    input_connections: typing.Dict[str, typing.List[str]] = Field(
        description="A map that explains what elements connect to this steps inputs. A connection could either be a Pipeline input (indicated by the '__pipeline__' token), or another steps output."
    )
    output_connections: typing.Dict[str, typing.List[str]] = Field(
        description="A map that explains what elemnts connect to this steps outputs. A connection could be either a Pipeline output, or another steps input."
    )


class PipelineStructureDesc(BaseModel):
    """Outlines the internal structure of a [Pipeline][kiara.pipeline.pipeline.Pipeline]."""

    class Config:
        allow_mutation = False
        extra = Extra.forbid

    pipeline_id: str = Field(description="The (unique) pipeline id.")
    steps: typing.Dict[str, StepDesc] = Field(
        description="The steps contained in this pipeline, with the 'step_id' as key."
    )
    processing_stages: typing.List[typing.List[str]] = Field(
        description="The order in which this pipeline has to be processed (basically the dependencies of each step on other steps, if any)."
    )
    pipeline_input_connections: typing.Dict[str, typing.List[str]] = Field(
        description="The connections of this pipelines input fields. One input field can be connected to one or several step input fields."
    )
    pipeline_output_connections: typing.Dict[str, str] = Field(
        description="The connections of this pipelines output fields. Each pipeline output is connected to exactly one step output field."
    )
