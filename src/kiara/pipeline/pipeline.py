# -*- coding: utf-8 -*-
import logging
import typing
import uuid
from rich.console import Console, ConsoleOptions, RenderResult

from kiara.data.registry import DataRegistry
from kiara.data.values import ValueSlot
from kiara.data.values.value_set import SlottedValueSet, ValueSet
from kiara.defaults import SpecialValue
from kiara.events import (
    PipelineInputEvent,
    PipelineOutputEvent,
    StepEvent,
    StepInputEvent,
    StepOutputEvent,
)
from kiara.pipeline import PipelineValuesInfo, StepStatus
from kiara.pipeline.controller import PipelineController
from kiara.pipeline.controller.batch import BatchController
from kiara.pipeline.listeners import PipelineListener
from kiara.pipeline.structure import PipelineStep, PipelineStructure
from kiara.pipeline.values import (
    PipelineInputRef,
    PipelineOutputRef,
    StepInputRef,
    StepOutputRef,
    ValueRef,
)

if typing.TYPE_CHECKING:
    from kiara.info.pipelines import PipelineState
    from kiara.kiara import Kiara

log = logging.getLogger("kiara")


class Pipeline(object):
    """An instance of a [PipelineStructure][kiara.pipeline.structure.PipelineStructure] that holds state for all of the inputs/outputs of the steps within."""

    def __init__(
        self,
        structure: PipelineStructure,
        # constants: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        controller: typing.Optional[PipelineController] = None,
        title: typing.Optional[str] = None,
    ):

        self._id: str = str(uuid.uuid4())
        if title is None:
            title = self._id

        self._title: str = title
        self._structure: PipelineStructure = structure

        self._pipeline_inputs: ValueSet = None  # type: ignore
        self._pipeline_outputs: ValueSet = None  # type: ignore

        self._step_inputs: typing.Mapping[str, ValueSet] = None  # type: ignore
        self._step_outputs: typing.Mapping[str, ValueSet] = None  # type: ignore

        self._value_refs: typing.Mapping[ValueSlot, typing.Iterable[ValueRef]] = None  # type: ignore
        self._status: StepStatus = StepStatus.STALE

        self._kiara: "Kiara" = self._structure._kiara
        self._data_registry: DataRegistry = self._kiara.data_registry

        self._init_values()

        if controller is None:
            controller = BatchController(self, kiara=self._kiara)
        else:
            controller.set_pipeline(self)
        self._controller: PipelineController = controller

        self._listeners: typing.List[PipelineListener] = []

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
    def title(self) -> str:
        return self._title

    @property
    def structure(self) -> PipelineStructure:
        return self._structure

    @property
    def controller(self) -> PipelineController:
        if self._controller is None:
            raise Exception("No controller set (yet).")
        return self._controller

    @property
    def inputs(self) -> ValueSet:
        return self._pipeline_inputs

    @property
    def outputs(self) -> ValueSet:
        return self._pipeline_outputs

    # def set_pipeline_inputs(self, **inputs: typing.Any):
    #     self._controller.set_pipeline_inputs(**inputs)

    @property
    def step_ids(self) -> typing.Iterable[str]:
        return self._structure.step_ids

    def get_step(self, step_id: str) -> PipelineStep:
        return self._structure.get_step(step_id)

    def get_step_inputs(self, step_id: str) -> ValueSet:
        return self._step_inputs[step_id]

    def get_step_outputs(self, step_id: str) -> ValueSet:
        return self._step_outputs[step_id]

    def add_listener(self, listener: PipelineListener) -> None:
        self._listeners.append(listener)

    @property
    def status(self) -> StepStatus:
        return self._state

    def _update_status(self):

        if self.inputs is None:
            new_state = StepStatus.STALE
        elif not self.inputs.items_are_valid():
            new_state = StepStatus.STALE
        elif not self.outputs.items_are_valid():
            new_state = StepStatus.INPUTS_READY
        else:
            new_state = StepStatus.RESULTS_READY

        self._state = new_state

    def _init_values(self):
        """Initialize this object. This should only be called once.

        Basically, this goes through all the inputs and outputs of all steps, and 'allocates' a PipelineValueInfo object
        for each of them. In case where output/input or pipeline-input/input points are connected, only one
        value item is allocated, since those refer to the same value.
        """

        pipeline_inputs: typing.Dict[str, ValueSlot] = {}
        pipeline_outputs: typing.Dict[str, ValueSlot] = {}

        all_step_inputs: typing.Dict[str, typing.Dict[str, ValueSlot]] = {}
        all_step_outputs: typing.Dict[str, typing.Dict[str, ValueSlot]] = {}

        value_refs: typing.Dict[ValueSlot, typing.List[ValueRef]] = {}

        # create the value objects that are associated with step outputs
        # all pipeline outputs are created here too, since the only place
        # those can be associated are step outputs
        for step_id, step_details in self._structure.steps_details.items():

            step_outputs: typing.Mapping[str, StepOutputRef] = step_details["outputs"]

            for output_name, output_point in step_outputs.items():

                init_output_value_item = self._data_registry.register_data(
                    value_schema=output_point.value_schema
                )
                output_value_slot = self._data_registry.register_alias(
                    value_or_schema=init_output_value_item, callbacks=[self]
                )
                value_refs.setdefault(output_value_slot, []).append(output_point)

                all_step_outputs.setdefault(step_id, {})[
                    output_name
                ] = output_value_slot

                # not all step outputs necessarily need to be connected to a pipeline output
                if output_point.pipeline_output:

                    pipeline_outputs[output_point.pipeline_output] = output_value_slot
                    po = self._structure.pipeline_outputs[output_point.pipeline_output]
                    value_refs.setdefault(output_value_slot, []).append(po)

        # create the value objects that are associated with step inputs
        for step_id, step_details in self._structure.steps_details.items():

            step_inputs: typing.Mapping[str, StepInputRef] = step_details["inputs"]

            for input_name, input_point in step_inputs.items():

                # if this step input gets fed from a pipeline_input (meaning user input in most cases),
                # we need to create a DataValue for that pipeline input
                # vm = ValueMetadata(
                #     origin=f"{self.id}.steps.{step_id}.inputs.{input_point.value_name}"
                # )
                if input_point.connected_pipeline_input:
                    connected_pipeline_input_name = input_point.connected_pipeline_input
                    pipeline_input_field: PipelineInputRef = (
                        self._structure.pipeline_inputs[connected_pipeline_input_name]
                    )
                    pipeline_input_slot: ValueSlot = pipeline_inputs.get(
                        connected_pipeline_input_name, None
                    )

                    if pipeline_input_slot is None:
                        # if the pipeline input wasn't created by another step input before,
                        # we need to take care of it here

                        if pipeline_input_field.is_constant:
                            init_value = self.structure.constants[
                                pipeline_input_field.value_name
                            ]
                        else:
                            init_value = self.structure.defaults.get(
                                pipeline_input_field.value_name, SpecialValue.NOT_SET
                            )

                        init_pipeline_input_value = self._data_registry.register_data(
                            value_data=init_value,
                            value_schema=pipeline_input_field.value_schema,
                        )
                        # TODO: check whether it's a constant?
                        pipeline_input_slot = self._data_registry.register_alias(
                            value_or_schema=init_pipeline_input_value, callbacks=[self]
                        )
                        value_refs.setdefault(pipeline_input_slot, []).append(
                            pipeline_input_field
                        )

                        pipeline_inputs[
                            connected_pipeline_input_name
                        ] = pipeline_input_slot

                    all_step_inputs.setdefault(step_id, {})[
                        input_name
                    ] = pipeline_input_slot
                    value_refs.setdefault(pipeline_input_slot, []).append(input_point)

                elif input_point.connected_outputs:

                    for co in input_point.connected_outputs:
                        if len(input_point.connected_outputs) == 1 and not co.sub_value:
                            # this means the input is the same value as the connected output
                            output_value: ValueSlot = all_step_outputs[co.step_id][
                                co.value_name
                            ]
                            all_step_inputs.setdefault(input_point.step_id, {})[
                                input_point.value_name
                            ] = output_value
                            value_refs.setdefault(output_value, []).append(input_point)
                        else:
                            print(input_point.connected_outputs)
                            raise NotImplementedError()
                            # sub_value = co.sub_value

                            # linked_values = {}
                            # for co in input_point.connected_outputs:
                            #     output_value = all_step_outputs[co.step_id][co.value_name]
                            #     sub_value = co.sub_value
                            #     if len(input_point.connected_outputs) > 1 and not sub_value:
                            #         raise NotImplementedError()
                            #         sub_value = {"config": co.step_id}
                            #     if sub_value is not None:
                            #         raise NotImplementedError
                            #
                            #     linked_values[output_value.id] = sub_value
                            #
                            # step_input = self._data_registry.register_linked_value(
                            #     parent_id=self.id,
                            #     linked_values=linked_values,
                            #     value_schema=input_point.value_schema,
                            #     value_refs=input_point,
                            # )
                            # self._data_registry.register_callback(
                            #     self.values_updated, step_input
                            # )
                            # all_step_inputs.setdefault(input_point.step_id, {})[
                            #     input_point.value_name
                            # ] = step_input

                else:
                    raise Exception(
                        f"Invalid value point type for this location: {input_point}"
                    )

        if not pipeline_inputs:
            raise Exception(f"Can't init pipeline '{self.title}': no pipeline inputs")

        self._pipeline_inputs = SlottedValueSet(
            items=pipeline_inputs,
            read_only=False,
            title=f"Inputs for pipeline '{self.title}'",
            kiara=self._kiara,
            registry=self._data_registry,
        )
        if not pipeline_outputs:
            raise Exception(f"Can't init pipeline '{self.title}': no pipeline outputs")

        self._pipeline_outputs = SlottedValueSet(
            items=pipeline_outputs,
            read_only=True,
            title=f"Outputs for pipeline '{self.title}'",
            kiara=self._kiara,
            registry=self._data_registry,
        )
        self._step_inputs = {}
        for step_id, inputs in all_step_inputs.items():
            self._step_inputs[step_id] = SlottedValueSet(
                items=inputs,
                read_only=True,
                title=f"Inputs for step '{step_id}' of pipeline '{self.title}",
                kiara=self._kiara,
                registry=self._data_registry,
            )
        self._step_outputs = {}
        for step_id, outputs in all_step_outputs.items():
            self._step_outputs[step_id] = SlottedValueSet(
                read_only=False,
                items=outputs,
                title=f"Outputs for step '{step_id}' of pipeline '{self.title}'",
                kiara=self._kiara,
                registry=self._data_registry,
            )

        self._value_refs = value_refs

    def values_updated(self, *items: ValueSlot) -> None:

        updated_inputs: typing.Dict[str, typing.List[str]] = {}
        updated_outputs: typing.Dict[str, typing.List[str]] = {}
        updated_pipeline_inputs: typing.List[str] = []
        updated_pipeline_outputs: typing.List[str] = []

        # print("===================================================")
        # for item in items:
        #     print(item)
        # print("===================================================")

        self._update_status()

        if self._value_refs is None:
            # means init is not finished yet
            return

        for item in items:

            # TODO: multiple value fields, also check pipeline id
            references = self._value_refs.get(item, None)
            assert references

            for p in references:

                if isinstance(p, StepInputRef):
                    updated_inputs.setdefault(p.step_id, []).append(p.value_name)
                elif isinstance(p, StepOutputRef):
                    updated_outputs.setdefault(p.step_id, []).append(p.value_name)
                elif isinstance(p, PipelineInputRef):
                    updated_pipeline_inputs.append(p.value_name)
                elif isinstance(p, PipelineOutputRef):
                    updated_pipeline_outputs.append(p.value_name)
                else:
                    raise TypeError(f"Can't update, invalid type: {type(p)}")

        # print('========================================')
        # print('---')
        # print("Upaded pipeline input")
        # print(updated_pipeline_inputs)
        # print('---')
        # print("Upaded step inputs")
        # print(updated_inputs)
        # print('---')
        # print("Upaded step outputs")
        # print(updated_outputs)
        # print('---')
        # print("Upaded pipeline outputs")
        # print(updated_pipeline_outputs)

        if updated_pipeline_inputs:
            event_pi = PipelineInputEvent(
                pipeline_id=self.id,
                updated_pipeline_inputs=updated_pipeline_inputs,
            )
            self._controller.pipeline_inputs_changed(event_pi)
            self._notify_pipeline_listeners(event_pi)

        if updated_outputs:
            event_so = StepOutputEvent(
                pipeline_id=self.id,
                updated_step_outputs=updated_outputs,
            )
            self._controller.step_outputs_changed(event_so)
            self._notify_pipeline_listeners(event_so)

        if updated_inputs:
            event_si = StepInputEvent(
                pipeline_id=self.id,
                updated_step_inputs=updated_inputs,
            )
            self._controller.step_inputs_changed(event_si)
            self._notify_pipeline_listeners(event_si)

        if updated_pipeline_outputs:
            event_po = PipelineOutputEvent(
                pipeline_id=self.id,
                updated_pipeline_outputs=updated_pipeline_outputs,
            )
            self._controller.pipeline_outputs_changed(event_po)
            self._notify_pipeline_listeners(event_po)

    def _notify_pipeline_listeners(self, event: StepEvent):

        for listener in self._listeners:
            if event.type == "step_input":  # type: ignore
                listener.step_inputs_changed(event)  # type: ignore
            elif event.type == "step_output":  # type: ignore
                listener.step_outputs_changed(event)  # type: ignore
            elif event.type == "pipeline_input":  # type: ignore
                listener.pipeline_inputs_changed(event)  # type: ignore
            elif event.type == "pipeline_output":  # type: ignore
                listener.pipeline_outputs_changed(event)  # type: ignore
            else:
                raise Exception(f"Unsupported type: {event.type}")  # type: ignore

    def get_current_state(self) -> "PipelineState":

        step_inputs = {}
        step_states = {}
        for k, v in self._step_inputs.items():
            step_inputs[k] = PipelineValuesInfo.from_value_set(v)
            if v.items_are_valid():
                step_states[k] = StepStatus.INPUTS_READY
            else:
                step_states[k] = StepStatus.STALE

        step_outputs = {}
        for k, v in self._step_outputs.items():
            step_outputs[k] = PipelineValuesInfo.from_value_set(v)
            if v.items_are_valid():
                step_states[k] = StepStatus.RESULTS_READY

        from kiara.info.pipelines import PipelineState

        state = PipelineState(
            structure=self.structure.to_details(),
            pipeline_inputs=self._pipeline_inputs.to_details(),
            pipeline_outputs=self._pipeline_outputs.to_details(),
            step_states=step_states,
            step_inputs=step_inputs,
            step_outputs=step_outputs,
            status=self.status,
        )
        return state

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield self.get_current_state()
