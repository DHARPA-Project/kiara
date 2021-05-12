# -*- coding: utf-8 -*-
import networkx as nx
import typing
import uuid
from functools import lru_cache
from networkx import NetworkXNoPath, NodeNotFound
from pydantic import BaseModel, Extra, Field, PrivateAttr
from rich import box
from rich.console import Console, ConsoleOptions, RenderGroup, RenderResult
from rich.panel import Panel
from rich.table import Table

from kiara.data.values import (
    PipelineInputField,
    PipelineOutputField,
    StepInputField,
    StepOutputField,
    StepValueAddress,
    ValueSchema,
    generate_step_alias,
)
from kiara.defaults import DEFAULT_NO_DESC_VALUE, PIPELINE_PARENT_MARKER, SpecialValue
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
    required: bool = Field(
        description="Whether this step is required within the workflow.\n\nIn some cases, when none of the pipeline outputs have a required input that connects to a step, then it is not necessary for this step to have been executed, even if it is placed before a step in the execution hierarchy. This also means that the pipeline inputs that are connected to this step might not be required.",
        default=True,
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
    _id: str = PrivateAttr()

    def __init__(self, **data):  # type: ignore

        self._id = str(uuid.uuid4())
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

        return self._id == other._id

        # # TODO: also check whether _kiara obj is equal?
        # eq = (self.step_id, self.parent_id, self.module, self.processing_stage,) == (
        #     other.step_id,
        #     other.parent_id,
        #     other.module,
        #     other.processing_stage,
        # )
        #
        # if not eq:
        #     return False
        #
        # hs = DeepHash(self.input_links)
        # ho = DeepHash(other.input_links)
        #
        # return hs[self.input_links] == ho[other.input_links]

    def __hash__(self):

        return hash(self._id)

        # # TODO: also include _kiara obj?
        # # TODO: figure out whether that can be made to work without deephash
        # hs = DeepHash(self.input_links)
        # return hash(
        #     (
        #         self.step_id,
        #         self.parent_id,
        #         self.module,
        #         self.processing_stage,
        #         hs[self.input_links],
        #     )
        # )

    def __repr__(self):

        return f"{self.__class__.__name__}(step_id={self.step_id} parent={self.parent_id} module_type={self.module_type} processing_stage={self.processing_stage})"

    def __str__(self):
        return f"step: {self.step_id} (module: {self.module_type})"


def generate_pipeline_endpoint_name(step_id: str, value_name: str):

    return f"{step_id}__{value_name}"


ALLOWED_INPUT_ALIAS_MARKERS = ["auto", "auto_all_outputs"]


def calculate_shortest_field_aliases(
    steps: typing.List[PipelineStep], alias_type: str, alias_for: str
):

    assert alias_for in ["inputs", "outputs"]
    if alias_type == "auto_all_outputs":

        aliases: typing.Dict[str, typing.List[str]] = {}

        for step in steps:

            if alias_for == "inputs":
                field_names = step.module.input_names
            else:
                field_names = step.module.output_names

            for field_name in field_names:
                aliases.setdefault(field_name, []).append(step.step_id)

        result = {}
        for field_name, step_ids in aliases.items():
            if len(step_ids) == 1:
                result[
                    generate_pipeline_endpoint_name(step_ids[0], field_name)
                ] = field_name
            else:
                for step_id in step_ids:
                    generated = generate_pipeline_endpoint_name(step_id, field_name)
                    result[generated] = generated

    elif alias_type == "auto":

        aliases = {}

        for stage_nr, step in enumerate(steps):

            _field_names: typing.Optional[typing.Iterable[str]] = None
            if alias_for == "inputs":
                _field_names = step.module.input_names
            else:
                if stage_nr == len(steps) - 1:
                    _field_names = step.module.output_names

            if not _field_names:
                continue

            for field_name in _field_names:
                aliases.setdefault(field_name, []).append(step.step_id)

        result = {}
        for field_name, step_ids in aliases.items():
            if len(step_ids) == 1:
                result[
                    generate_pipeline_endpoint_name(step_ids[0], field_name)
                ] = field_name
            else:
                for step_id in step_ids:
                    generated = generate_pipeline_endpoint_name(step_id, field_name)
                    result[generated] = generated

    return result


class PipelineStructure(object):
    """An object that holds one or several steps, and describes the connections between them."""

    def __init__(
        self,
        parent_id: str,
        steps: typing.Iterable["PipelineStepConfig"],
        input_aliases: typing.Union[str, typing.Mapping[str, str]] = None,
        output_aliases: typing.Union[str, typing.Mapping[str, str]] = None,
        add_all_workflow_outputs: bool = False,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if not steps:
            raise Exception("No steps provided.")

        if kiara is None:
            kiara = Kiara.instance()
        self._kiara: Kiara = kiara
        self._steps: typing.List[PipelineStep] = PipelineStep.create_steps(
            parent_id, *steps, kiara=self._kiara
        )
        self._pipeline_id: str = parent_id

        if input_aliases is None:
            input_aliases = {}

        if isinstance(input_aliases, str):
            if input_aliases not in ALLOWED_INPUT_ALIAS_MARKERS:
                raise Exception(
                    f"Can't create pipeline, invalid value '{input_aliases}' for 'input_aliases'. Either specify a dict, or use one of: {', '.join(ALLOWED_INPUT_ALIAS_MARKERS)}"
                )

            input_aliases = calculate_shortest_field_aliases(
                self._steps, input_aliases, "inputs"
            )

        if isinstance(output_aliases, str):
            if output_aliases not in ALLOWED_INPUT_ALIAS_MARKERS:
                raise Exception(
                    f"Can't create pipeline, invalid value '{output_aliases}' for 'output_aliases'. Either specify a dict, or use one of: {', '.join(ALLOWED_INPUT_ALIAS_MARKERS)}"
                )

            output_aliases = calculate_shortest_field_aliases(
                self._steps, output_aliases, "outputs"
            )

        self._input_aliases: typing.Mapping[str, str] = input_aliases  # type: ignore
        if output_aliases is None:
            output_aliases = {}
        self._output_aliases: typing.Mapping[str, str] = output_aliases  # type: ignore

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

    @property
    def step_ids(self) -> typing.Iterable[str]:
        if self._steps_details is None:
            self._process_steps()
        return self._steps_details.keys()

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
        pipeline_outputs: typing.Dict[str, PipelineOutputField] = {}
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
                    pipeline_outputs[step_output_name] = pipeline_output
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

        # calculating which steps are always required to execute to compute one of the required pipeline outputs.
        # this is done because in some cases it's possible that some steps can be skipped to execute if they
        # don't have a valid input set, because the inputs downstream they are connecting to are 'non-required'
        # optional_steps = []

        last_stage = self._processing_stages[-1]

        step_nodes: typing.List[PipelineStep] = [
            node
            for node in self._data_flow_graph_simple.nodes
            if isinstance(node, PipelineStep)
        ]

        all_required_inputs = []
        for step_id in last_stage:

            step = self.get_step(step_id)
            step_nodes.remove(step)

            for s_inp in self.get_step_inputs(step_id).values():
                if not s_inp.value_schema.is_required():
                    continue
                all_required_inputs.append(s_inp)

        for pipeline_input in self.pipeline_inputs.values():

            for last_step_input in all_required_inputs:
                try:
                    path = nx.shortest_path(
                        self._data_flow_graph_simple, pipeline_input, last_step_input
                    )
                    for p in path:
                        if p in step_nodes:
                            step_nodes.remove(p)
                except (NetworkXNoPath, NodeNotFound):
                    pass
                    # print("NO PATH")
                    # print(f"{pipeline_input} -> {last_step_input}")

        for s in step_nodes:
            s.required = False

        for input_name, inp in self.pipeline_inputs.items():
            steps = set()
            for ci in inp.connected_inputs:
                steps.add(ci.step_id)

            optional = True
            for step_id in steps:
                step = self.get_step(step_id)
                if step.required:
                    optional = False
                    break
            if optional:
                inp.value_schema.optional = True

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
                required=step.required,
            )

        return PipelineStructureDesc(
            pipeline_id=self._pipeline_id,
            steps=steps,
            processing_stages=self.processing_stages,
            pipeline_input_connections=workflow_inputs,
            pipeline_output_connections=workflow_outputs,
            pipeline_inputs=self.pipeline_inputs,
            pipeline_outputs=self.pipeline_outputs,
        )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        d = self.to_details()
        yield d


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
        description="""A map that explains what elements connect to this steps inputs. A connection could either be a Pipeline input (indicated by the ``__pipeline__`` token), or another steps output.

Example:
``` json
input_connections: {
    "a": ["__pipeline__.a"],
    "b": ["step_one.a"]
}

```
        """
    )
    output_connections: typing.Dict[str, typing.List[str]] = Field(
        description="A map that explains what elemnts connect to this steps outputs. A connection could be either a Pipeline output, or another steps input."
    )
    required: bool = Field(
        description="Whether this step is always required, or potentially could be skipped in case some inputs are not available."
    )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield f"[b]Step: {self.step.step_id}[\b]"


class StepsInfo(BaseModel):

    pipeline_id: str = Field(description="The pipeline id.")
    steps: typing.Dict[str, StepDesc] = Field(description="A list of step details.")
    processing_stages: typing.List[typing.List[str]] = Field(
        description="The stages in which the steps are processed."
    )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        explanation = {}

        for nr, stage in enumerate(self.processing_stages):

            stage_details = {}
            for step_id in stage:
                step: StepDesc = self.steps[step_id]
                if step.required:
                    title = step_id
                else:
                    title = f"{step_id} (optional)"
                stage_details[title] = step.step.module.doc()

            explanation[nr + 1] = stage_details

        lines = []
        for stage_nr, stage_steps in explanation.items():
            lines.append(f"[bold]Processing stage {stage_nr}[/bold]:")
            lines.append("")
            for step_id, desc in stage_steps.items():
                desc
                if desc == DEFAULT_NO_DESC_VALUE:
                    lines.append(f"  - {step_id}")
                else:
                    lines.append(f"  - {step_id}: [i]{desc}[/i]")
                lines.append("")

        padding = (1, 2, 0, 2)
        yield Panel(
            "\n".join(lines),
            box=box.ROUNDED,
            title_align="left",
            title=f"Stages for pipeline: [b]{self.pipeline_id}[/b]",
            padding=padding,
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
    pipeline_inputs: typing.Dict[str, PipelineInputField] = Field(
        description="The pipeline inputs."
    )
    pipeline_outputs: typing.Dict[str, PipelineOutputField] = Field(
        description="The pipeline outputs."
    )

    @property
    def steps_info(self) -> StepsInfo:

        return StepsInfo(
            pipeline_id=self.pipeline_id,
            processing_stages=self.processing_stages,
            steps=self.steps,
        )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield f"[b]Pipeline structure: {self.pipeline_id}[/b]\n"

        yield "[b]Inputs / Outputs[/b]"

        data_panel: typing.List[typing.Any] = []
        inp_table = Table(show_header=True, box=box.SIMPLE, show_lines=True)
        inp_table.add_column("Name", style="i")
        inp_table.add_column("Type")
        inp_table.add_column("Description")
        inp_table.add_column("Required", justify="center")
        inp_table.add_column("Default", justify="center")

        for inp, details in self.pipeline_inputs.items():
            req = details.value_schema.is_required()
            if not req:
                req_str = "no"
            else:
                d = details.value_schema.default
                if d in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]:
                    req_str = "[b]yes[/b]"
                else:
                    req_str = "no"
            default = details.value_schema.default
            if default in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]:
                default = "-- no default --"
            else:
                default = str(default)
            inp_table.add_row(
                inp,
                details.value_schema.type,
                details.value_schema.doc,
                req_str,
                default,
            )

        p_inp = Panel(
            inp_table, box=box.ROUNDED, title="Input fields", title_align="left"
        )
        data_panel.append(p_inp)

        # yield "[b]Pipeline outputs[/b]"

        out_table = Table(show_header=True, box=box.SIMPLE, show_lines=True)
        out_table.add_column("Name", style="i")
        out_table.add_column("Type")
        out_table.add_column("Description")

        for inp, details_o in self.pipeline_outputs.items():

            out_table.add_row(
                inp,
                details_o.value_schema.type,
                details_o.value_schema.doc,
            )

        outp = Panel(
            out_table, box=box.ROUNDED, title="Output fields", title_align="left"
        )
        data_panel.append(outp)
        yield Panel(RenderGroup(*data_panel), box=box.SIMPLE)

        color_list = [
            "green",
            "blue",
            "bright_magenta",
            "dark_red",
            "gold3",
            "cyan",
            "orange1",
            "light_yellow3",
            "light_slate_grey",
            "deep_pink4",
        ]

        step_color_map = {}
        for i, s in enumerate(self.steps.values()):
            step_color_map[s.step.step_id] = color_list[i % len(color_list)]

        rg = []
        for nr, stage in enumerate(self.processing_stages):

            render_group = []

            for s in self.steps.values():

                if s.step.step_id not in stage:
                    continue

                step_table = create_step_table(s, step_color_map)
                render_group.append(step_table)

            panel = Panel(
                RenderGroup(*render_group),
                box=box.ROUNDED,
                title=f"Processing stage: {nr+1}",
                title_align="left",
            )
            rg.append(panel)

        yield "[b]Steps[/b]"
        r_panel = Panel(RenderGroup(*rg), box=box.SIMPLE)
        yield r_panel


def create_step_table(
    step_desc: StepDesc, step_color_map: typing.Mapping[str, str]
) -> Table:

    step = step_desc.step

    table = Table(show_header=True, box=box.SIMPLE, show_lines=False)
    table.add_column("step_id:", style="i", no_wrap=True)
    c = step_color_map[step.step_id]
    table.add_column(f"[b {c}]{step.step_id}[/b {c}]", no_wrap=True)

    doc_link = step.module.doc_link()
    if doc_link:
        module_str = f"[link={doc_link}]{step.module_type}[/link]"
    else:
        module_str = step.module_type

    table.add_row("", f"\n{step.module.doc()}\n")
    table.add_row("type", module_str)

    table.add_row(
        "required", "[red]yes[/red]" if step.required else "[green]no[/green]"
    )
    table.add_row("is pipeline", "yes\n" if step.module.is_pipeline() else "no\n")

    input_links: typing.List[typing.Any] = []
    max_source_len = 0
    for source, targets in step_desc.input_connections.items():
        source_type = step_desc.step.module.input_schemas[source].type
        source = f"{source} ([i]type: {source_type}[/i])"
        source_len = len(source)
        if source_len > max_source_len:
            max_source_len = source_len
        for i, target in enumerate(targets):
            if i == 0:
                input_links.append((source, target))
            else:
                input_links.append((None, target))

    last_source = None
    for i, il in enumerate(input_links):
        source = il[0]
        if source is None:
            padding = (
                len(last_source) - 6
            ) * " "  # calculate without the [i]..[/i] markers
            source_str = padding + "  "
        else:
            last_source = source.ljust(max_source_len)
            source_str = last_source + " ← "
        target = il[1]
        tokens = target.split(".")
        assert len(tokens) == 2
        if tokens[0] == PIPELINE_PARENT_MARKER:
            target_str = f"[b]PIPE_INPUT[/b].{tokens[1]}"
        else:
            c = step_color_map[tokens[0]]
            target_str = f"[b {c}]{tokens[0]}[/b {c}].{tokens[1]}"

        postfix = ""
        if len(input_links) == i + 1:
            postfix = "\n"
        if i == 0:
            row_str = f"{source_str}{target_str}{postfix}"
            table.add_row("inputs", row_str)
        else:
            row_str = f"{source_str}{target_str}{postfix}"
            table.add_row("", row_str)

    output_links: typing.List[typing.Any] = []
    max_source_len = 0
    for source, targets in step_desc.output_connections.items():
        target_type = step_desc.step.module.output_schemas[source].type
        source = f"{source} ([i]type: {target_type}[/i])"
        source_len = len(source)
        if source_len > max_source_len:
            max_source_len = source_len
        for i, target in enumerate(targets):
            if i == 0:
                output_links.append((source, target))
            else:
                output_links.append((None, target))

    last_source = None
    for i, il in enumerate(output_links):
        source = il[0]
        if source is None:
            padding = (
                len(last_source) - 6
            ) * " "  # calculate without the [i]..[/i] markers
            source_str = padding + "  "
        else:
            last_source = source.ljust(max_source_len)
            source_str = last_source + " → "
        target = il[1]
        tokens = target.split(".")
        assert len(tokens) == 2
        if tokens[0] == PIPELINE_PARENT_MARKER:
            target_str = f"[b]PIPE_OUTPUT[/b].{tokens[1]}"
        else:
            c = step_color_map[tokens[0]]
            target_str = f"[b {c}]{tokens[0]}[/b {c}].{tokens[1]}"
        if i == 0:
            row_str = f"{source_str}{target_str}"
            table.add_row("outputs", row_str)
        else:
            row_str = f"{source_str}{target_str}"
            table.add_row("", row_str)

    return table
