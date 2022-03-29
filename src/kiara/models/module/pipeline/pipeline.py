# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import logging
import networkx as nx
import sys
import uuid
from typing import (
    Any,
    Dict,
    Hashable,
    Iterable,
    List,
    Mapping,
    Optional,
)

from kiara.defaults import SpecialValue
from kiara.exceptions import InputValuesException
from kiara.kiara import DataRegistry
from kiara.kiara.aliases import AliasValueMap
from kiara.models.module.pipeline import StepStatus
from kiara.models.module.pipeline.structure import PipelineStep, PipelineStructure
from kiara.models.module.pipeline.value_refs import (
    PipelineInputRef,
    PipelineOutputRef,
    StepInputRef,
    StepOutputRef,
    ValueRef,
)
from kiara.models.values.value import ORPHAN, Value

log = logging.getLogger("kiara")

import abc

from kiara.models.events import ChangedValue
from kiara.models.events.pipeline import (
    PipelineEvent,
    PipelineInputEvent,
    PipelineOutputEvent,
    StepInputEvent,
    StepOutputEvent,
)


class PipelineListener(abc.ABC):
    @abc.abstractmethod
    def get_listener_id(self) -> Hashable:
        pass

    @abc.abstractmethod
    def get_subscribed_event_types(self) -> Iterable[str]:
        pass

    @abc.abstractmethod
    def _pipeline_events_occurred(self, *events: PipelineEvent):
        pass


class BasePipelineListener(PipelineListener):
    def _pipeline_events_occurred(self, *events: PipelineEvent):

        for event in events:

            event_type: str = event.event_type  # type: ignore

            if event_type == "pipeline_input":
                self.pipeline_inputs_changed(event=event)  # type: ignore
            elif event_type == "pipeline_output":
                self.pipeline_outputs_changed(event=event)  # type: ignore
            elif event_type == "step_input":
                self.step_inputs_changed(event=event)  # type: ignore
            elif event_type == "step_output":
                self.step_outputs_changed(event=event)  # type: ignore
            else:
                raise Exception(f"Invalid event type: {event_type}")

    def get_subscribed_event_types(self) -> Iterable[str]:
        return ["pipeline_input", "step_input", "step_output", "pipeline_output"]

    def step_inputs_changed(self, event: StepInputEvent):
        """Method to override if the implementing controller needs to react to events where one or several step inputs have changed.

        Arguments:
            event: the step input event
        """

    def step_outputs_changed(self, event: StepOutputEvent):
        """Method to override if the implementing controller needs to react to events where one or several step outputs have changed.

        Arguments:
            event: the step output event
        """

    def pipeline_inputs_changed(self, event: PipelineInputEvent):
        """Method to override if the implementing controller needs to react to events where one or several pipeline inputs have changed.

        !!! note
        Whenever pipeline inputs change, the connected step inputs also change and an (extra) event will be fired for those. Which means
        you can choose to only implement the ``step_inputs_changed`` method if you want to. This behaviour might change in the future.

        Arguments:
            event: the pipeline input event
        """

    def pipeline_outputs_changed(self, event: PipelineOutputEvent):
        """Method to override if the implementing controller needs to react to events where one or several pipeline outputs have changed.

        Arguments:
            event: the pipeline output event
        """


class Pipeline(object):
    """An instance of a [PipelineStructure][kiara.pipeline.structure.PipelineStructure] that holds state for all of the inputs/outputs of the steps within."""

    def __init__(self, structure: PipelineStructure, data_registry: DataRegistry):

        self._id: uuid.UUID = uuid.uuid4()

        self._structure: PipelineStructure = structure

        self._value_refs: Mapping[AliasMap, Iterable[ValueRef]] = None  # type: ignore
        self._status: StepStatus = StepStatus.STALE

        self._steps_by_stage: Dict[int, Dict[str, PipelineStep]] = None  # type: ignore
        self._inputs_by_stage: Dict[int, List[str]] = None  # type: ignore
        self._outputs_by_stage: Dict[int, List[str]] = None  # type: ignore

        self._data_registry: DataRegistry = data_registry

        self._all_values: AliasValueMap = None  # type: ignore

        self._init_values()

        self._listeners: Dict[str, List[PipelineListener]] = {}

        # self._update_status()

    def _init_values(self):
        """Initialize this object. This should only be called once.

        Basically, this goes through all the inputs and outputs of all steps, and 'allocates' a PipelineValueInfo object
        for each of them. In case where output/input or pipeline-input/input points are connected, only one
        value item is allocated, since those refer to the same value.
        """

        values = AliasValueMap(
            alias=str(self.id), version=0, assoc_value=None, values_schema={}
        )
        values._data_registry = self._data_registry
        for field_name, schema in self._structure.pipeline_inputs_schema.items():
            values.set_alias_schema(f"pipeline.inputs.{field_name}", schema=schema)
        for field_name, schema in self._structure.pipeline_outputs_schema.items():
            values.set_alias_schema(f"pipeline.outputs.{field_name}", schema=schema)
        for step_id in self.step_ids:
            step = self.get_step(step_id)
            for field_name, value_schema in step.module.inputs_schema.items():
                values.set_alias_schema(
                    f"steps.{step_id}.inputs.{field_name}", schema=value_schema
                )
            for field_name, value_schema in step.module.outputs_schema.items():
                values.set_alias_schema(
                    f"steps.{step_id}.outputs.{field_name}", schema=value_schema
                )

        self._all_values = values
        self._all_values.print_tree()
        return

    def __eq__(self, other):

        if not isinstance(other, Pipeline):
            return False

        return self._id == other._id

    def __hash__(self):

        return hash(self._id)

    def add_listener(self, listener: PipelineListener):

        event_types = listener.get_subscribed_event_types()
        if isinstance(event_types, str):
            event_types = [event_types]
        for event_type in event_types:
            self._listeners.setdefault(event_type, []).append(listener)

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def structure(self) -> PipelineStructure:
        return self._structure

    def get_current_pipeline_inputs(self) -> Dict[str, Optional[uuid.UUID]]:
        """All (pipeline) input values of this pipeline."""

        alias_map = self._all_values.get_alias("pipeline.inputs")
        return alias_map.get_all_value_ids()  # type: ignore

    def get_current_pipeline_outputs(self) -> Dict[str, Optional[uuid.UUID]]:
        """All (pipeline) output values of this pipeline."""

        alias_map = self._all_values.get_alias("pipeline.outputs")
        return alias_map.get_all_value_ids()  # type: ignore

    def get_current_step_inputs(self, step_id) -> Dict[str, Optional[uuid.UUID]]:

        alias_map = self._all_values.get_alias(f"steps.{step_id}.inputs")
        return alias_map.get_all_value_ids()  # type: ignore

    def get_current_step_outputs(self, step_id) -> Dict[str, Optional[uuid.UUID]]:

        alias_map = self._all_values.get_alias(f"steps.{step_id}.inputs")
        return alias_map.get_all_value_ids()  # type: ignore

    def get_inputs_for_steps(
        self, *step_ids: str
    ) -> Dict[str, Dict[str, Optional[uuid.UUID]]]:
        """Retrieve value ids for the inputs of the specified steps (or all steps, if no argument provided."""

        result = {}
        for step_id in self._structure.step_ids:
            if step_ids and step_id not in step_ids:
                continue
            ids = self.get_current_step_inputs(step_id=step_id)
            result[step_id] = ids
        return result

    def get_outputs_for_steps(
        self, *step_ids: str
    ) -> Dict[str, Dict[str, Optional[uuid.UUID]]]:
        """Retrieve value ids for the outputs of the specified steps (or all steps, if no argument provided."""

        result = {}
        for step_id in self._structure.step_ids:
            if step_ids and step_id not in step_ids:
                continue
            ids = self.get_current_step_outputs(step_id=step_id)
            result[step_id] = ids
        return result

    def _notify_pipeline_listeners(self, *events: PipelineEvent):

        listener_event_map: Dict[Hashable, List[PipelineEvent]] = {}
        listener_id_map: Dict[Hashable, PipelineListener] = {}
        for event in events:
            for event_type, listeners in self._listeners.items():
                for l in listeners:
                    if event == event_type:
                        listener_event_map.setdefault(l.get_listener_id(), []).append(
                            event
                        )
                        if l.get_listener_id() not in listener_id_map.keys():
                            listener_id_map[l.get_listener_id()] = l

        for l_id, events in listener_event_map.items():
            listener = listener_id_map[l_id]
            listener._pipeline_events_occurred(*events)

    def set_pipeline_inputs(
        self, _sync_to_step_inputs: bool = False, **inputs: Any
    ) -> Mapping[str, ChangedValue]:

        values_to_set: Dict[str, Optional[uuid.UUID]] = {}

        for k, v in inputs.items():
            if v is None:
                values_to_set[k] = None
            else:
                alias_map = self._all_values.get_alias("pipeline.inputs")
                assert alias_map is not None
                schema = alias_map.values_schema[k]
                value = self._data_registry.register_data(
                    data=v, schema=schema, pedigree=ORPHAN
                )
                values_to_set[k] = value.value_id

        changed_pipeline_inputs = self._set_values("pipeline.inputs", **values_to_set)

        events = []
        event: PipelineEvent = PipelineInputEvent.construct(
            kiara_id=self._data_registry._kiara.id,
            pipeline_id=self.id,
            changed_inputs=changed_pipeline_inputs,
        )
        events.append(event)

        if _sync_to_step_inputs:
            changed = self.sync_pipeline_inputs(_notify_listeners=False)
            for step_id, details in changed.items():
                event = StepInputEvent.construct(
                    kiara_id=self._data_registry._kiara.id,
                    pipeline_id=self.id,
                    step_id=step_id,
                    changed_inputs=details,
                )
                events.append(event)

        self._notify_pipeline_listeners(event)
        return changed_pipeline_inputs

    def sync_pipeline_inputs(
        self, _notify_listeners: bool = True
    ) -> Mapping[str, Mapping[str, ChangedValue]]:

        pipeline_inputs = self.get_current_pipeline_inputs()

        values_to_sync: Dict[str, Dict[str, Optional[uuid.UUID]]] = {}

        for field_name, ref in self._structure.pipeline_input_refs.items():
            for step_input in ref.connected_inputs:
                step_inputs = self.get_current_step_inputs(step_input.step_id)

                if step_inputs[step_input.value_name] != pipeline_inputs[field_name]:
                    values_to_sync.setdefault(step_input.step_id, {})[
                        step_input.value_name
                    ] = pipeline_inputs[field_name]

        results: Dict[str, Mapping[str, ChangedValue]] = {}
        for step_id in values_to_sync.keys():
            values = values_to_sync[step_id]
            step_changed = self.set_step_inputs(
                step_id=step_id, _send_event=False, **values
            )
            results[step_id] = step_changed

        if _notify_listeners:
            events = []
            for step_id, changed in results.items():
                event = StepInputEvent.construct(
                    kiara_id=self._data_registry._kiara.id,
                    pipeline_id=self.id,
                    step_id=step_id,
                    changed_inputs=changed,
                )
                events.append(event)
            self._notify_pipeline_listeners(*events)

        return results

    def set_step_inputs(
        self, step_id: str, _send_event: bool = True, **inputs: Optional[uuid.UUID]
    ) -> Mapping[str, ChangedValue]:

        changed_step_inputs = self._set_values(f"steps.{step_id}.inputs", **inputs)
        if not changed_step_inputs:
            return

        step = self._structure.get_step(step_id)

        for node in list(nx.dfs_successors(self._structure.data_flow_graph, step)):
            if isinstance(node, StepInputRef):
                pass
            elif isinstance(node, StepOutputRef):
                pass
            elif isinstance(node, PipelineOutputRef):
                pass

        if _send_event:
            event = StepInputEvent.construct(
                kiara_id=self._data_registry._kiara.id,
                pipeline_id=self.id,
                step_id=step_id,
                changed_inputs=changed_step_inputs,
            )
            self._notify_pipeline_listeners(event)
        return changed_step_inputs

    def _set_values(
        self, alias: str, **values: Optional[uuid.UUID]
    ) -> Dict[str, ChangedValue]:
        """Set values (value-ids) for the sub-alias-map with the specified alias path."""

        invalid = {}
        for k in values.keys():
            if k not in self._all_values.get_alias(alias).values_schema.keys():
                invalid[
                    k
                ] = f"Invalid field '{k}'. Available fields: {', '.join(self.current_pipeline_inputs.values_schema.keys())}"

        if invalid:
            raise InputValuesException(invalid_inputs=invalid)

        alias_map: AliasValueMap = self._all_values.get_alias(alias)
        assert alias_map is not None

        values_to_set: Dict[str, Optional[uuid.UUID]] = {}
        current: Dict[str, Optional[uuid.UUID]] = {}
        changed: Dict[str, ChangedValue] = {}

        for field_name, new_value in values.items():

            current_value = self._all_values.get_alias(f"{alias}.{field_name}")
            if current_value is not None:
                current_value = current_value.assoc_value
            current[field_name] = current_value

            if current_value != new_value:
                values_to_set[field_name] = new_value
                changed[field_name] = ChangedValue(old=current_value, new=new_value)

        self._all_values.get_alias(alias).set_aliases(**values_to_set)

        return changed

    @property
    def step_ids(self) -> Iterable[str]:
        """Return all ids of the steps of this pipeline."""
        return self._structure.step_ids

    def get_step(self, step_id: str) -> PipelineStep:
        """Return the object representing a step in this workflow, identified by the step id."""
        return self._structure.get_step(step_id)

    def get_steps_by_stage(
        self,
    ) -> Mapping[int, Mapping[str, PipelineStep]]:
        """Return a all pipeline steps, ordered by stage they belong to."""

        if self._steps_by_stage is not None:
            return self._steps_by_stage

        result: Dict[int, Dict[str, PipelineStep]] = {}
        for step_id in self.step_ids:
            step = self.get_step(step_id)
            stage = self._structure.get_processing_stage(step.step_id)
            assert stage is not None
            result.setdefault(stage, {})[step_id] = step

        self._steps_by_stage = result
        return self._steps_by_stage


class PipelineOld(object):
    def get_pipeline_inputs_by_stage(self) -> Mapping[int, Iterable[str]]:
        """Return a list of pipeline input names, ordered by stage they are first required."""

        if self._inputs_by_stage is not None:
            return self._inputs_by_stage

        result: Dict[int, List[str]] = {}
        for k, v in self.inputs._value_slots.items():  # type: ignore
            refs = self._value_refs[v]
            min_stage = sys.maxsize
            for ref in refs:
                if not isinstance(ref, StepInputRef):
                    continue
                step = self.get_step(ref.step_id)
                stage = self._structure.get_processing_stage(step.step_id)
                assert stage is not None
                if stage < min_stage:
                    min_stage = stage  # type: ignore
                result.setdefault(min_stage, []).append(k)

        self._inputs_by_stage = result
        return self._inputs_by_stage

    def get_pipeline_outputs_by_stage(
        self,
    ) -> Mapping[int, Iterable[str]]:
        """Return a list of pipeline input names, ordered by the stage that needs to be executed before they are available."""

        raise NotImplementedError()
        if self._outputs_by_stage is not None:
            return self._outputs_by_stage

        result: Dict[int, List[str]] = {}
        for k, v in self.outputs._value_slots.items():  # type: ignore
            refs = self._value_refs[v]
            min_stage = sys.maxsize
            for ref in refs:
                if not isinstance(ref, StepOutputRef):
                    continue
                step = self.get_step(ref.step_id)
                stage = self._structure.get_processing_stage(step.step_id)
                assert stage is not None
                if stage < min_stage:
                    min_stage = stage  # type: ignore
                result.setdefault(min_stage, []).append(k)

        self._outputs_by_stage = result
        return self._outputs_by_stage

    def get_pipeline_inputs_for_stage(self, stage: int) -> Iterable[str]:
        """Return a list of pipeline inputs that are required for a stage to be processed.

        The result of this method does not include inputs that were required earlier already.
        """

        return self.get_pipeline_inputs_by_stage().get(stage, [])

    def get_stage_for_pipeline_input(self, input_name: str) -> int:

        for stage, input_names in self.get_pipeline_inputs_by_stage().items():
            if input_name in input_names:
                return stage

        raise Exception(
            f"No input name '{input_name}'. Available inputs: {', '.join(self.inputs.keys())}"
        )

    def stage_for_pipeline_output(self, output_name: str) -> int:

        for stage, output_names in self.get_pipeline_outputs_by_stage().items():
            if output_name in output_names:
                return stage

        raise Exception(
            f"No output name '{output_name}'. Available outputs: {', '.join(self.outputs.keys())}"
        )

    def get_pipeline_outputs_for_stage(self, stage: int) -> Iterable[str]:
        """Return a list of pipeline outputs that are first available after the specified stage completed processing."""

        return self.get_pipeline_outputs_by_stage().get(stage, [])

    def get_pipeline_inputs_for_step(self, step_id: str) -> List[str]:

        result = []
        for field_name, value_slot in self.inputs._value_slots.items():
            refs = self._value_refs[value_slot]
            for ref in refs:
                if not isinstance(ref, PipelineInputRef):
                    continue
                for ci in ref.connected_inputs:
                    if ci.step_id == step_id and ref.value_name not in result:
                        result.append(ref.value_name)

        return result

    def get_pipeline_outputs_for_step(self, step_id: str) -> List[str]:

        result = []
        for field_name, value_slot in self.outputs._value_slots.items():
            refs = self._value_refs[value_slot]
            for ref in refs:
                if not isinstance(ref, PipelineOutputRef):
                    continue
                if (
                    ref.connected_output.step_id == step_id
                    and ref.value_name not in result
                ):
                    result.append(ref.value_name)

        return result

    # def get_step_inputs(self, step_id: str) -> ValueSet:
    #     """Return all inputs for a step id (incl. inputs that are not pipeline inputs but connected to other modules output)."""
    #     return self._step_inputs[step_id]
    #
    # def get_step_outputs(self, step_id: str) -> ValueSet:
    #     """Return all outputs for a step id (incl. outputs that are not pipeline outputs)."""
    #     return self._step_outputs[step_id]

    def add_listener(self, listener: PipelineListener) -> None:
        """Add a listener taht gets notified on any internal pipeline input/output events."""
        self._listeners.append(listener)

    @property
    def status(self) -> StepStatus:
        """Return the current status of this pipeline."""
        return self._state

    def _update_status(self):
        """Make sure internal state variable is up to date."""

        if self.inputs is None:
            new_state = StepStatus.STALE
        elif not self.inputs.items_are_valid():
            new_state = StepStatus.STALE
        elif not self.outputs.items_are_valid():
            new_state = StepStatus.INPUTS_READY
        else:
            new_state = StepStatus.RESULTS_READY

        self._state = new_state

        values.print_tree()

        pipeline_inputs: Dict[str, ValueSlot] = {}
        pipeline_outputs: Dict[str, ValueSlot] = {}

        all_step_inputs: Dict[str, Dict[str, ValueSlot]] = {}
        all_step_outputs: Dict[str, Dict[str, ValueSlot]] = {}

        value_refs: Dict[ValueSlot, List[ValueRef]] = {}

        # create the value objects that are associated with step outputs
        # all pipeline outputs are created here too, since the only place
        # those can be associated are step outputs
        for step_id, step_details in self._structure.steps_details.items():

            step_outputs: Mapping[str, StepOutputRef] = step_details["outputs"]

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
                    po = self._structure.pipeline_output_refs[
                        output_point.pipeline_output
                    ]
                    value_refs.setdefault(output_value_slot, []).append(po)

        # create the value objects that are associated with step inputs
        for step_id, step_details in self._structure.steps_details.items():

            step_inputs: Mapping[str, StepInputRef] = step_details["inputs"]

            for input_name, input_point in step_inputs.items():

                # if this step input gets fed from a pipeline_input (meaning user input in most cases),
                # we need to create a DataValue for that pipeline input
                # vm = ValueMetadata(
                #     origin=f"{self.id}.steps.{step_id}.inputs.{input_point.value_name}"
                # )
                if input_point.connected_pipeline_input:
                    connected_pipeline_input_name = input_point.connected_pipeline_input
                    pipeline_input_field: PipelineInputRef = (
                        self._structure.pipeline_input_refs[
                            connected_pipeline_input_name
                        ]
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
        self._steps_by_stage = None
        self._inputs_by_stage = None

    def values_updated(self, *items: Mapping[str, Value]) -> None:

        updated_inputs: Dict[str, List[str]] = {}
        updated_outputs: Dict[str, List[str]] = {}
        updated_pipeline_inputs: List[str] = []
        updated_pipeline_outputs: List[str] = []

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

    def _notify_pipeline_listeners(self, event: PipelineEvent):

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

        raise NotImplementedError()
        #
        # step_inputs = {}
        # step_states = {}
        # for k, v in self._step_inputs.items():
        #     step_inputs[k] = PipelineValuesInfo.from_value_set(v)
        #     if v.items_are_valid():
        #         step_states[k] = StepStatus.INPUTS_READY
        #     else:
        #         step_states[k] = StepStatus.STALE
        #
        # step_outputs = {}
        # for k, v in self._step_outputs.items():
        #     step_outputs[k] = PipelineValuesInfo.from_value_set(v)
        #     if v.items_are_valid():
        #         step_states[k] = StepStatus.RESULTS_READY
        #
        # from kiara.info.pipelines import PipelineState
        #
        # state = PipelineState(
        #     structure=self.structure.to_details(),
        #     pipeline_inputs=self._pipeline_inputs.to_details(),
        #     pipeline_outputs=self._pipeline_outputs.to_details(),
        #     step_states=step_states,
        #     step_inputs=step_inputs,
        #     step_outputs=step_outputs,
        #     status=self.status,
        # )
        # return state
