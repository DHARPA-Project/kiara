# -*- coding: utf-8 -*-
import networkx as nx
from functools import lru_cache
from networkx import NetworkXNoPath, NodeNotFound
from pydantic import Field, PrivateAttr, root_validator
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Set

from kiara.defaults import PIPELINE_STRUCTURE_TYPE_CATEGORY_ID
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

if TYPE_CHECKING:
    # from kiara.info.pipelines import PipelineStructureDesc
    from kiara.models.module.pipeline import PipelineConfig


# class StepValueAddress(BaseModel):
#     """Small model to describe the address of a value of a step, within a Pipeline/PipelineStructure."""
#
#     class Config:
#         extra = Extra.forbid
#
#     step_id: str = Field(description="The id of a step within a pipeline.")
#     value_name: str = Field(
#         description="The name of the value (output name or pipeline input name)."
#     )
#     sub_value: Optional[Dict[str, Any]] = Field(
#         default=None,
#         description="A reference to a subitem of a value (e.g. column, list item)",
#     )
#
#     @property
#     def alias(self):
#         """An alias string for this address (in the form ``[step_id].[value_name]``)."""
#         return generate_step_alias(self.step_id, self.value_name)
#
#     def __eq__(self, other):
#
#         if not isinstance(other, StepValueAddress):
#             return False
#
#         return (self.step_id, self.value_name, self.sub_value) == (
#             other.step_id,
#             other.value_name,
#             other.sub_value,
#         )
#
#     def __hash__(self):
#
#         return hash((self.step_id, self.value_name, self.sub_value))
#
#     def __repr__(self):
#
#         if self.sub_value:
#             sub_value = f" sub_value={self.sub_value}"
#         else:
#             sub_value = ""
#         return f"StepValueAddres(step_id={self.step_id}, value_name={self.value_name}{sub_value})"
#
#     def __str__(self):
#         return self.__repr__()


# class PipelineStep(Manifest):
#     """A step within a pipeline-structure, includes information about it's connection(s) and other metadata."""
#
#     class Config:
#         validate_assignment = True
#         extra = Extra.forbid
#
#     @classmethod
#     def create_steps(
#         cls, *steps: "PipelineStepConfig", kiara: "Kiara"
#     ) -> List["PipelineStep"]:
#
#         result: List[PipelineStep] = []
#
#
#         for step in steps:
#
#             type_cls = kiara.get_module_class(step.module_type)
#             input_links = {}
#             for input_field, sources  in step.get("input_links", {}).items():
#                 if isinstance(sources, str):
#                     sources = [sources]
#                     input_links[input_field] = sources
#
#             print("----")
#             dbg(input_links)
#             _s = PipelineStep(
#                 step_id=step.step_id,
#                 module_type=step.module_type,
#                 module_config=copy.deepcopy(step.module_config),
#                 input_links=input_links,
#                 module_class=PythonClass.from_class(type_cls)
#             )
#             result.append(_s)
#
#         return result
#
#     @validator("step_id")
#     def _validate_step_id(cls, v):
#
#         assert isinstance(v, str)
#         if "." in v:
#             raise ValueError("Step ids can't contain '.' characters.")
#
#         return v
#
#     step_id: str = Field(description="Locally unique id (within a pipeline) of this step.")
#
#     module_type: str = Field(description="The module type.")
#     module_config: Dict[str, Any] = Field(
#         description="The module config.", default_factory=dict
#     )
#     # required: bool = Field(
#     #     description="Whether this step is required within the workflow.\n\nIn some cases, when none of the pipeline outputs have a required input that connects to a step, then it is not necessary for this step to have been executed, even if it is placed before a step in the execution hierarchy. This also means that the pipeline inputs that are connected to this step might not be required.",
#     #     default=True,
#     # )
#     # processing_stage: Optional[int] = Field(
#     #     default=None,
#     #     description="The stage number this step is executed within the pipeline.",
#     # )
#     input_links: Mapping[str, List[StepValueAddress]] = Field(
#         description="The links that connect to inputs of the module.",
#         default_factory=list,
#     )
#     module_class: PythonClass = Field(description="The class of the underlying module.")
#     _module: Optional["KiaraModule"] = PrivateAttr(default=None)
#
#     def _retrieve_data_to_hash(self) -> Any:
#         return self.dict()
#
#     def _retrieve_id(self) -> str:
#         return str(self.model_data_hash)
#
#     def _retrieve_category_id(self) -> str:
#         return PIPELINE_STEP_TYPE_CATEGORY_ID
#
#     # def __init__(self, **data):  # type: ignore
#     #
#     #     self._id: uuid.UUID = uuid.uuid4()
#     #     kiara = data.pop("_kiara", None)
#     #     if kiara is None:
#     #         from kiara import Kiara
#     #
#     #         kiara = Kiara.instance()
#     #
#     #     super().__init__(**data)
#     #     self._kiara: "Kiara" = kiara
#     #
#
#     @property
#     def module(self) -> "KiaraModule":
#         if self._module is None:
#             m_cls = self.module_class.get_class()
#             self._module = m_cls(module_config=self.module_config)
#         return self._module


def generate_pipeline_endpoint_name(step_id: str, value_name: str):

    return f"{step_id}__{value_name}"


def calculate_shortest_field_aliases(
    steps: List[PipelineStep], alias_type: str, alias_for: str
):
    """Utility method to figure out the best field aliases automatically."""

    assert alias_for in ["inputs", "outputs"]
    if alias_type == "auto_all_outputs":

        aliases: Dict[str, List[str]] = {}

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

            _field_names: Optional[Iterable[str]] = None
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


ALLOWED_INPUT_ALIAS_MARKERS = ["auto", "auto_all_outputs"]


class PipelineStructure(KiaraModel):
    """An object that holds one or several steps, and describes the connections between them."""

    pipeline_config: PipelineConfig = Field(
        description="The underlying pipeline config."
    )
    steps: List[PipelineStep] = Field(description="The pipeline steps ")
    input_aliases: Dict[str, str] = Field(description="The (resolved) input aliases.")
    output_aliases: Dict[str, str] = Field(description="The (resolved) output aliases.")

    @root_validator(pre=True)
    def validate_pipeline_config(cls, values):

        pipeline_config = values.get("pipeline_config", None)
        if not pipeline_config:
            raise ValueError("No 'pipeline_config' provided.")

        if len(values) != 1:
            raise ValueError(
                "Only 'pipeline_config' key allowed when creating a pipeline structure object."
            )

        _config: PipelineConfig = pipeline_config
        _steps: List[PipelineStep] = list(_config.steps)

        if not _config.input_aliases:
            input_aliases = {}
        else:
            if isinstance(_config.input_aliases, str):
                if _config.input_aliases not in ALLOWED_INPUT_ALIAS_MARKERS:
                    raise Exception(
                        f"Can't create pipeline, invalid value '{_config.input_aliases}' for 'input_aliases'. Either specify a dict, or use one of: {', '.join(ALLOWED_INPUT_ALIAS_MARKERS)}"
                    )

                input_aliases = calculate_shortest_field_aliases(
                    _steps, _config.input_aliases, "inputs"
                )
            else:
                input_aliases = dict(_config.input_aliases)

        _input_aliases: Dict[str, str] = dict(input_aliases)  # type: ignore

        if not _config.output_aliases:
            output_aliases = {}
        else:
            if isinstance(_config.output_aliases, str):
                if _config.output_aliases not in ALLOWED_INPUT_ALIAS_MARKERS:
                    raise Exception(
                        f"Can't create pipeline, invalid value '{_config.output_aliases}' for 'output_aliases'. Either specify a dict, or use one of: {', '.join(ALLOWED_INPUT_ALIAS_MARKERS)}"
                    )

                output_aliases = calculate_shortest_field_aliases(
                    _steps, _config.output_aliases, "outputs"
                )
            else:
                output_aliases = dict(_config.output_aliases)

        _output_aliases: Dict[str, str] = output_aliases

        values["steps"] = _steps
        values["input_aliases"] = input_aliases
        values["output_aliases"] = output_aliases
        return values

    # this is hardcoded for now
    _add_all_workflow_outputs: bool = PrivateAttr(default=False)
    _constants: Dict[str, Any] = PrivateAttr(default=None)  # type: ignore
    _defaults: Dict[str, Any] = PrivateAttr(None)  # type: ignore

    _execution_graph: nx.DiGraph = PrivateAttr(None)  # type: ignore
    _data_flow_graph: nx.DiGraph = PrivateAttr(None)  # type: ignore
    _data_flow_graph_simple: nx.DiGraph = PrivateAttr(None)  # type: ignore

    _processing_stages: List[List[str]] = PrivateAttr(None)  # type: ignore

    # holds details about the (current) processing steps contained in this workflow
    _steps_details: Dict[str, Any] = PrivateAttr(None)  # type: ignore

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "steps": [step.model_data_hash for step in self.steps],
            "input_aliases": self.input_aliases,
            "output_aliases": self.output_aliases,
        }

    def _retrieve_id(self) -> str:
        return self.pipeline_config.model_id

    def _retrieve_category_id(self) -> str:
        return PIPELINE_STRUCTURE_TYPE_CATEGORY_ID

    @property
    def steps_details(self) -> Mapping[str, Any]:

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
            raise Exception(f"No module with id: {step_id}")

        return d["step"]

    def get_step_input_refs(self, step_id: str) -> Mapping[str, StepInputRef]:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No module with id: {step_id}")

        return d["inputs"]

    def get_step_output_refs(self, step_id: str) -> Mapping[str, StepOutputRef]:

        d = self.steps_details.get(step_id, None)
        if d is None:
            raise Exception(f"No module with id: {step_id}")

        return d["outputs"]

    def get_step_details(self, step_id: str) -> Mapping[str, Any]:

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
    def processing_stages(self) -> List[List[str]]:
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

        return {
            input_name: w_in.value_schema
            for input_name, w_in in self.pipeline_input_refs.items()
        }

    @property
    def pipeline_outputs_schema(self) -> Mapping[str, ValueSchema]:
        return {
            output_name: w_out.value_schema
            for output_name, w_out in self.pipeline_output_refs.items()
        }

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

        return self.get_step_details(step_id=step_id)["required"]

    def _process_steps(self):
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

            # go through all the module outputs, create points for them and connect them to pipeline outputs
            for output_name, schema in step.module.outputs_schema.items():

                step_output = StepOutputRef(
                    value_name=output_name,
                    value_schema=schema,
                    step_id=step.step_id,
                )

                steps_details[step.step_id]["outputs"][output_name] = step_output
                step_alias = generate_step_alias(step.step_id, output_name)
                outputs[step_alias] = step_output

                step_output_name = generate_pipeline_endpoint_name(
                    step_id=step.step_id, value_name=output_name
                )
                if self.output_aliases:
                    if step_output_name in self.output_aliases.keys():
                        step_output_name = self.output_aliases[step_output_name]
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
                    pipeline_input_name = generate_pipeline_endpoint_name(
                        step_id=step.step_id, value_name=input_name
                    )
                    # check whether this input has an alias associated with it
                    if self.input_aliases:
                        if pipeline_input_name in self.input_aliases.keys():
                            # this means we use the pipeline alias
                            pipeline_input_name = self.input_aliases[
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

        max_length = max(path_lengths.values())

        for i in range(1, max_length + 1):
            stage: List[str] = [m for m, length in path_lengths.items() if length == i]
            processing_stages.append(stage)
            for _step_id in stage:
                steps_details[_step_id]["processing_stage"] = i
                # steps_details[_step_id]["step"].processing_stage = i

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

        step_nodes: List[PipelineStep] = [
            node
            for node in self._data_flow_graph_simple.nodes
            if isinstance(node, PipelineStep)
        ]

        all_required_inputs = []
        for step_id in last_stage:

            step = self.get_step(step_id)
            step_nodes.remove(step)

            for k, s_inp in self.get_step_input_refs(step_id).items():
                if not s_inp.value_schema.is_required():
                    continue
                all_required_inputs.append(s_inp)

        for pipeline_input in self.pipeline_input_refs.values():

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
            self._steps_details[s.step_id]["required"] = False
            # s.required = False

        for input_name, inp in self.pipeline_input_refs.items():
            steps = set()
            for ci in inp.connected_inputs:
                steps.add(ci.step_id)

            optional = True
            for step_id in steps:
                step = self.get_step(step_id)
                if self._steps_details[step_id]["required"]:
                    optional = False
                    break
            if optional:
                inp.value_schema.optional = True
