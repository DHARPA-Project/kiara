# -*- coding: utf-8 -*-
import copy
import networkx as nx
import typing
import uuid
from functools import lru_cache
from networkx import NetworkXNoPath, NodeNotFound
from pydantic import BaseModel, Extra, Field, PrivateAttr, validator
from rich.console import Console, ConsoleOptions, RenderResult

from kiara.data.values import ValueSchema
from kiara.pipeline import StepValueAddress
from kiara.pipeline.utils import extend_pipeline, generate_step_alias
from kiara.pipeline.values import (
    PipelineInputField,
    PipelineOutputField,
    StepInputField,
    StepOutputField,
)

if typing.TYPE_CHECKING:
    from kiara.info.pipelines import PipelineStructureDesc
    from kiara.kiara import Kiara
    from kiara.module import KiaraModule
    from kiara.pipeline.config import PipelineModuleConfig, PipelineStepConfig
    from kiara.pipeline.pipeline import Pipeline


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
                module_config=copy.deepcopy(step.module_config),
                input_links=copy.deepcopy(step.input_links),
                _kiara=kiara,
            )
            result.append(_s)

        return result

    _module: typing.Optional["KiaraModule"] = PrivateAttr(default=None)

    @validator("step_id")
    def _validate_step_id(cls, v):

        assert isinstance(v, str)
        if "." in v:
            raise ValueError("Step ids can't contain '.' characters.")

        return v

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
        self._kiara: "Kiara" = kiara

    @property
    def kiara(self):
        return self._kiara

    @property
    def module(self) -> "KiaraModule":

        if self._module is None:

            try:
                if self.module_type in self.kiara.operation_mgmt.profiles.keys():
                    op = self.kiara.operation_mgmt.profiles[self.module_type]
                    self._module = op.module
                else:
                    self._module = self.kiara.create_module(
                        id=self.step_id,
                        module_type=self.module_type,
                        module_config=self.module_config,
                    )
            except Exception as e:
                raise Exception(
                    f"Can't assemble pipeline structure '{self.parent_id}': {e}"
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
        config: "PipelineModuleConfig",
        kiara: typing.Optional["Kiara"] = None,
    ):

        self._structure_config: "PipelineModuleConfig" = config

        steps = self._structure_config.steps
        input_aliases = self._structure_config.input_aliases
        output_aliases = self._structure_config.output_aliases

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

        self._input_aliases: typing.Dict[str, str] = dict(input_aliases)  # type: ignore
        if output_aliases is None:
            output_aliases = {}
        self._output_aliases: typing.Dict[str, str] = dict(output_aliases)  # type: ignore

        # this is hardcoded for now
        self._add_all_workflow_outputs: bool = False

        self._constants: typing.Dict[str, typing.Any] = None  # type: ignore
        self._defaults: typing.Dict[str, typing.Any] = None  # type: ignore

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
    def structure_config(self) -> "PipelineModuleConfig":
        return self._structure_config

    @property
    def steps(self) -> typing.Iterable[PipelineStep]:
        return self._steps

    @property
    def modules(self) -> typing.Iterable["KiaraModule"]:
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

    @property
    def constants(self) -> typing.Mapping[str, typing.Any]:

        if self._constants is None:
            self._process_steps()
        return self._constants

    @property
    def defaults(self) -> typing.Mapping[str, typing.Any]:

        if self._defaults is None:
            self._process_steps()
        return self._defaults

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
        constants = {}
        structure_defaults = {}

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

            module_constants: typing.Mapping[
                str, typing.Any
            ] = step.module.get_config_value("constants")

            for input_name, schema in step.module.input_schemas.items():

                matching_input_links: typing.List[StepValueAddress] = []
                is_constant = input_name in module_constants.keys()

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
                                f"Can't connect input '{input_name}' for step '{step.step_id}': no output '{output_id}' available. Available output names: {', '.join(outputs.keys())}"
                            )
                        connected_output_points.append(outputs[output_id])
                        connected_outputs.append(input_link)

                        other_step_dependency.add(input_link.step_id)

                    step_input_point = StepInputField(
                        step_id=step.step_id,
                        pipeline_id=self._pipeline_id,
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
                    pipeline_input_name = generate_pipeline_endpoint_name(
                        step_id=step.step_id, value_name=input_name
                    )
                    # check whether this input has an alias associated with it
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
                        assert connected_pipeline_input.is_constant == is_constant
                    else:
                        # we need to create the pipeline input
                        connected_pipeline_input = PipelineInputField(
                            value_name=pipeline_input_name,
                            value_schema=schema,
                            pipeline_id=self._pipeline_id,
                            is_constant=is_constant,
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

        self._constants = constants
        self._defaults = structure_defaults
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

    def extend(
        self,
        other: typing.Union[
            "Pipeline",
            "PipelineStructure",
            "PipelineModuleConfig",
            typing.Mapping[str, typing.Any],
        ],
        input_links: typing.Optional[
            typing.Mapping[str, typing.Iterable[StepValueAddress]]
        ] = None,
    ) -> "PipelineStructure":

        return extend_pipeline(self, other)

    def to_details(self) -> "PipelineStructureDesc":

        from kiara.info.pipelines import PipelineStructureDesc

        return PipelineStructureDesc.create_pipeline_structure_desc(pipeline=self)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        d = self.to_details()
        yield d
