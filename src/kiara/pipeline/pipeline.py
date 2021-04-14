# -*- coding: utf-8 -*-
import logging
import typing
import uuid
from enum import Enum
from pydantic import BaseModel, Field

from kiara.data.registry import DataRegistry
from kiara.data.values import (
    PipelineInputField,
    PipelineOutputField,
    PipelineValues,
    StepInputField,
    StepOutputField,
    Value,
    ValueSet,
)
from kiara.events import (
    PipelineInputEvent,
    PipelineOutputEvent,
    StepInputEvent,
    StepOutputEvent,
)
from kiara.pipeline.controller import BatchController, PipelineController
from kiara.pipeline.structure import (
    PipelineStep,
    PipelineStructure,
    PipelineStructureDesc,
)

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara

log = logging.getLogger("kiara")


class StepStatus(Enum):
    """Enum to describe the state of a workflow."""

    STALE = "stale"
    INPUTS_READY = "inputs_ready"
    RESULTS_INCOMING = "processing"
    RESULTS_READY = "results_ready"


class Pipeline(object):
    """An instance of a [PipelineStructure][kiara.pipeline.structure.PipelineStructure] that holds state for all of the inputs/outputs of the steps within."""

    def __init__(
        self,
        structure: PipelineStructure,
        constants: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        controller: typing.Optional[PipelineController] = None,
        data_registry: typing.Optional[DataRegistry] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        self._id: str = str(uuid.uuid4())
        self._structure: PipelineStructure = structure

        self._pipeline_inputs: ValueSet = None  # type: ignore
        self._pipeline_outputs: ValueSet = None  # type: ignore

        self._step_inputs: typing.Mapping[str, ValueSet] = None  # type: ignore
        self._step_outputs: typing.Mapping[str, ValueSet] = None  # type: ignore

        self._status: StepStatus = StepStatus.STALE

        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()
        self._kiara: Kiara = kiara

        if constants is None:
            constants = {}
        self._constants: typing.Mapping[str, typing.Any] = constants

        if data_registry is None:
            data_registry = self._kiara.data_registry
        self._data_registry: DataRegistry = data_registry

        self._init_values()

        if controller is None:
            controller = BatchController(self)
        else:
            controller.set_pipeline(self)
        self._controller: PipelineController = controller

        self._update_status()

    def __eq__(self, other):

        if not isinstance(other, Pipeline):
            return False

        return self._id == other._id

    def __hash__(self):

        return hash(self._id)

    @property
    def id(self) -> str:
        return self._id

    @property
    def structure(self) -> PipelineStructure:
        return self._structure

    @property
    def inputs(self) -> ValueSet:
        return self._pipeline_inputs

    @property
    def outputs(self) -> ValueSet:
        return self._pipeline_outputs

    # def set_pipeline_inputs(self, **inputs: typing.Any):
    #     self._controller.set_pipeline_inputs(**inputs)

    def get_step(self, step_id: str) -> PipelineStep:
        return self._structure.get_step(step_id)

    def get_step_inputs(self, step_id: str) -> ValueSet:
        return self._step_inputs[step_id]

    def get_step_outputs(self, step_id: str) -> ValueSet:
        return self._step_outputs[step_id]

    @property
    def status(self) -> StepStatus:
        return self._state

    def _update_status(self):

        if not self.inputs.items_are_valid:
            new_state = StepStatus.STALE
        elif not self.outputs.items_are_valid:
            new_state = StepStatus.INPUTS_READY
        else:
            new_state = StepStatus.RESULTS_READY

        self._state = new_state

    def _init_values(self):
        """Initialize this object. This should only be called once.

        Basically, this goes through all the inputs and outputs of all steps, and 'allocates' a PipelineValue object
        for each of them. In case where output/input or pipeline-input/input points are connected, only one
        value item is allocated, since those refer to the same value.
        """

        pipeline_inputs: typing.Dict[str, Value] = {}
        pipeline_outputs: typing.Dict[str, Value] = {}

        all_step_inputs: typing.Dict[str, typing.Dict[str, Value]] = {}
        all_step_outputs: typing.Dict[str, typing.Dict[str, Value]] = {}

        # create the value objects that are associated with step outputs
        # all pipeline outputs are created here too, since the only place
        # those can be associated are step outputs
        for step_id, step_details in self._structure.steps_details.items():

            step_outputs: typing.Mapping[str, StepOutputField] = step_details["outputs"]

            for output_name, output_point in step_outputs.items():

                output_value_item = self._data_registry.register_value(
                    value_schema=output_point.value_schema,
                    value_fields=output_point,
                    origin=f"step_output:{self.structure.pipeline_id}.{output_point.alias}",
                    is_constant=False,
                )
                self._data_registry.register_callback(
                    self.values_updated, output_value_item
                )
                all_step_outputs.setdefault(step_id, {})[
                    output_name
                ] = output_value_item

                # not all step outputs necessarily need to be connected to a pipeline output
                if output_point.pipeline_output:
                    po = self._structure.pipeline_outputs[output_point.pipeline_output]
                    pv = self._data_registry.register_linked_value(
                        output_value_item, value_fields=po
                    )
                    self._data_registry.register_callback(self.values_updated, pv)
                    pipeline_outputs[output_point.pipeline_output] = pv

        # create the value objects that are associated with step inputs
        for step_id, step_details in self._structure.steps_details.items():

            step_inputs: typing.Mapping[str, StepInputField] = step_details["inputs"]

            for input_name, input_point in step_inputs.items():

                # if this step input gets fed from a pipeline_input (meaning user input in most cases),
                # we need to create a DataValue for that pipeline input
                if input_point.connected_pipeline_input:
                    connected_pipeline_input_name = input_point.connected_pipeline_input
                    pipeline_input_field = self._structure.pipeline_inputs[
                        connected_pipeline_input_name
                    ]
                    pipeline_input = pipeline_inputs.get(
                        connected_pipeline_input_name, None
                    )

                    if pipeline_input is None:
                        # if the pipeline input wasn't created by another step input before,
                        # we need to take care of it here

                        constant = self._constants.get(
                            connected_pipeline_input_name, None
                        )
                        pipeline_input = self._data_registry.register_value(
                            value_schema=input_point.value_schema,
                            value_fields=pipeline_input_field,
                            is_constant=False if constant is None else True,
                            initial_value=constant,
                            origin=f"pipeline_input:{self.structure.pipeline_id}.{input_name}",
                        )
                        self._data_registry.register_callback(
                            self.values_updated, pipeline_input
                        )

                        pipeline_inputs[connected_pipeline_input_name] = pipeline_input
                        # TODO: create input field value
                    else:
                        # TODO: compare schemas of multiple inputs
                        log.warning(
                            "WARNING: not comparing schemas of pipeline inputs with links to more than one step input"
                        )
                        # raise NotImplementedError()

                    step_input = self._data_registry.register_linked_value(
                        linked_values=pipeline_input,
                        value_fields=input_point,
                        origin=f"step_input:{self.structure.pipeline_id}.{input_point.alias}",
                    )
                    self._data_registry.register_callback(
                        self.values_updated, step_input
                    )

                    all_step_inputs.setdefault(step_id, {})[input_name] = step_input

                elif input_point.connected_outputs:

                    if len(input_point.connected_outputs) > 1:
                        raise NotImplementedError()
                    for co in input_point.connected_outputs:
                        output_value = all_step_outputs[co.step_id][co.value_name]
                        step_input = self._data_registry.register_linked_value(
                            linked_values=output_value,
                            value_fields=input_point,
                            origin=f"step_input:{self.structure.pipeline_id}.{input_point.alias}",
                        )
                        self._data_registry.register_callback(
                            self.values_updated, step_input
                        )
                        all_step_inputs.setdefault(input_point.step_id, {})[
                            input_point.value_name
                        ] = step_input

                else:
                    raise Exception(
                        f"Invalid value point type for this location: {input_point}"
                    )

        self._pipeline_inputs = ValueSet(items=pipeline_inputs)
        self._pipeline_outputs = ValueSet(items=pipeline_outputs)
        self._step_inputs = {}
        for step_id, inputs in all_step_inputs.items():
            self._step_inputs[step_id] = ValueSet(items=inputs)
        self._step_outputs = {}
        for step_id, outputs in all_step_outputs.items():
            self._step_outputs[step_id] = ValueSet(items=outputs)

    def values_updated(self, *items: Value):

        updated_inputs: typing.Dict[str, typing.List[str]] = {}
        updated_outputs: typing.Dict[str, typing.List[str]] = {}
        updated_pipeline_inputs: typing.List[str] = []
        updated_pipeline_outputs: typing.List[str] = []

        # print("===================================================")
        # for item in items:
        #     print(item)
        # print("===================================================")

        self._update_status()

        for item in items:

            # TODO: multiple value fields, also check pipeline id
            ps = item.value_fields
            if len(ps) != 1:
                raise NotImplementedError()

            p = list(ps)[0]

            if isinstance(p, StepInputField):
                updated_inputs.setdefault(p.step_id, []).append(p.value_name)
            elif isinstance(p, StepOutputField):
                updated_outputs.setdefault(p.step_id, []).append(p.value_name)
            elif isinstance(p, PipelineInputField):
                updated_pipeline_inputs.append(p.value_name)
            elif isinstance(p, PipelineOutputField):
                updated_pipeline_outputs.append(p.value_name)
            else:
                raise TypeError(f"Can't update, invalid type: {type(p)}")

        if updated_pipeline_inputs:
            event_pi = PipelineInputEvent(
                pipeline_id=self._structure.pipeline_id,
                updated_pipeline_inputs=updated_pipeline_inputs,
            )
            self._controller.pipeline_inputs_changed(event_pi)

        if updated_inputs:
            event_si = StepInputEvent(
                pipeline_id=self._structure.pipeline_id,
                updated_step_inputs=updated_inputs,
            )
            self._controller.step_inputs_changed(event_si)

        if updated_outputs:
            event_so = StepOutputEvent(
                pipeline_id=self._structure.pipeline_id,
                updated_step_outputs=updated_outputs,
            )
            self._controller.step_outputs_changed(event_so)

        if updated_pipeline_outputs:
            event_po = PipelineOutputEvent(
                pipeline_id=self._structure.pipeline_id,
                updated_pipeline_outputs=updated_pipeline_outputs,
            )
            self._controller.pipeline_outputs_changed(event_po)

    def get_current_state(self) -> "PipelineState":

        step_inputs = {}
        for k, v in self._step_inputs.items():
            step_inputs[k] = PipelineValues.from_value_set(v)

        step_outputs = {}
        for k, v in self._step_outputs.items():
            step_outputs[k] = PipelineValues.from_value_set(v)

        state = PipelineState(
            structure=self.structure.to_details(),
            pipeline_inputs=self._pipeline_inputs.to_details(),
            pipeline_outputs=self._pipeline_outputs.to_details(),
            step_inputs=step_inputs,
            step_outputs=step_outputs,
            status=self.status,
        )
        return state


class PipelineState(BaseModel):
    """Describes the current state of a pipeline.

    This includes the structure of the pipeline (how the internal modules/steps are connected to each other), as well
    as all current input/output values for the pipeline itself, as well as for all internal steps.

    Use the ``dict`` or ``json`` methods to convert this object into a generic data structure.
    """

    structure: PipelineStructureDesc = Field(
        description="The structure (interconnections of modules/steps) of the pipeline."
    )
    pipeline_inputs: PipelineValues = Field(
        description="The current (externally facing) input values of this pipeline."
    )
    pipeline_outputs: PipelineValues = Field(
        description="The current (externally facing) output values of this pipeline."
    )
    step_inputs: typing.Dict[str, PipelineValues] = Field(
        description="The current (internal) input values of each step of this pipeline."
    )
    step_outputs: typing.Dict[str, PipelineValues] = Field(
        description="The current (internal) output values of each step of this pipeline."
    )
    status: StepStatus = Field(description="The current overal status of the pipeline.")
