# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import dpath
import networkx as nx
import uuid
from pydantic import Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Type, Union

from kiara.defaults import NONE_VALUE_ID, NOT_SET_VALUE_ID, SpecialValue
from kiara.exceptions import InvalidValuesException
from kiara.interfaces.python_api.models.info import ItemInfo
from kiara.models.aliases import AliasValueMap
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.events.pipeline import (
    ChangedValue,
    PipelineEvent,
    PipelineState,
    StepDetails,
)
from kiara.models.module.jobs import JobConfig
from kiara.models.module.pipeline import PipelineConfig, StepStatus
from kiara.models.module.pipeline.structure import PipelineStep, PipelineStructure
from kiara.models.module.pipeline.value_refs import ValueRef
from kiara.models.values.value import ORPHAN
from kiara.models.values.value_schema import ValueSchema
from kiara.registries.data import DataRegistry
from kiara.utils.operations import create_operation
from kiara.utils.output import (
    create_pipeline_steps_tree,
    create_table_from_model_object,
    create_value_map_status_renderable,
)
from kiara.utils.yaml import StringYAML

if TYPE_CHECKING:
    from kiara.context import Kiara

yaml = StringYAML()


class PipelineListener(abc.ABC):
    @abc.abstractmethod
    def _pipeline_event_occurred(self, event: PipelineEvent):
        pass


class Pipeline(object):
    """An instance of a [PipelineStructure][kiara.pipeline.structure.PipelineStructure] that holds state for all of the inputs/outputs of the steps within."""

    @classmethod
    def create_pipeline(
        cls,
        kiara: "Kiara",
        pipeline: Union[PipelineConfig, PipelineStructure, Mapping, str],
    ) -> "Pipeline":

        if isinstance(pipeline, Mapping):
            pipeline_structure: PipelineStructure = PipelineConfig.from_config(
                pipeline_name="__pipeline__", data=pipeline, kiara=kiara
            ).structure
        elif isinstance(pipeline, PipelineConfig):
            pipeline_structure = pipeline.structure
        elif isinstance(pipeline, PipelineStructure):
            pipeline_structure = pipeline
        elif isinstance(pipeline, str):
            operation = create_operation(module_or_operation=pipeline, kiara=kiara)
            module = operation.module
            if isinstance(module.config, PipelineConfig):
                config: PipelineConfig = module.config
            else:
                raise NotImplementedError()
            pipeline_structure = config.structure
        else:
            raise Exception(f"Invalid type for argument 'pipeline': {type(pipeline)}")

        pipeline_obj = Pipeline(kiara=kiara, structure=pipeline_structure)
        return pipeline_obj

    def __init__(self, structure: PipelineStructure, kiara: "Kiara"):

        self._id: uuid.UUID = uuid.uuid4()

        self._structure: PipelineStructure = structure

        self._value_refs: Mapping[AliasValueMap, Iterable[ValueRef]] = None  # type: ignore
        # self._status: StepStatus = StepStatus.STALE

        self._steps_by_stage: Dict[int, Dict[str, PipelineStep]] = None  # type: ignore
        self._inputs_by_stage: Dict[int, List[str]] = None  # type: ignore
        self._outputs_by_stage: Dict[int, List[str]] = None  # type: ignore

        self._kiara: Kiara = kiara
        self._data_registry: DataRegistry = kiara.data_registry

        self._all_values: AliasValueMap = None  # type: ignore

        self._listeners: List[PipelineListener] = []

        self._init_values()

        # self._update_status()

    @property
    def pipeline_id(self) -> uuid.UUID:
        return self._id

    # @property
    # def pipeline_name(self) -> str:
    #     return self.structure.pipeline_config.pipeline_name

    @property
    def kiara_id(self) -> uuid.UUID:
        return self._kiara.id

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
        inputs_schema = self._structure.pipeline_inputs_schema
        outputs_schema = self._structure.pipeline_outputs_schema
        if inputs_schema:
            for field_name, schema in inputs_schema.items():
                values.set_alias_schema(f"pipeline.inputs.{field_name}", schema=schema)
        else:
            values.set_alias_schema("pipeline.inputs", schema=ValueSchema(type="none"))
        if outputs_schema:
            for field_name, schema in outputs_schema.items():
                values.set_alias_schema(f"pipeline.outputs.{field_name}", schema=schema)
        else:
            values.set_alias_schema("pipeline.outputs", schema=ValueSchema(type="none"))
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

        initial_inputs = {
            k: SpecialValue.NOT_SET
            for k in self._structure.pipeline_inputs_schema.keys()
        }
        self.set_pipeline_inputs(inputs=initial_inputs)

    def __eq__(self, other):

        if not isinstance(other, Pipeline):
            return False

        return self._id == other._id

    def __hash__(self):

        return hash(self._id)

    def add_listener(self, listener: PipelineListener):

        self._listeners.append(listener)

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def structure(self) -> PipelineStructure:
        return self._structure

    @property
    def doc(self) -> DocumentationMetadataModel:
        return self.structure.pipeline_config.doc

    def get_current_pipeline_inputs(self) -> Dict[str, uuid.UUID]:
        """All (pipeline) input values of this pipeline."""

        if not self._structure.steps:
            return {}

        alias_map = self._all_values.get_alias("pipeline.inputs")
        return alias_map.get_all_value_ids()  # type: ignore

    def get_current_pipeline_outputs(self) -> Dict[str, uuid.UUID]:
        """All (pipeline) output values of this pipeline."""

        if not self._structure.steps:
            return {}

        alias_map = self._all_values.get_alias("pipeline.outputs")
        return alias_map.get_all_value_ids()  # type: ignore

    def get_current_step_inputs(self, step_id) -> Dict[str, uuid.UUID]:

        alias_map = self._all_values.get_alias(f"steps.{step_id}.inputs")
        return alias_map.get_all_value_ids()  # type: ignore

    def get_current_step_outputs(self, step_id) -> Dict[str, uuid.UUID]:

        alias_map = self._all_values.get_alias(f"steps.{step_id}.outputs")
        return alias_map.get_all_value_ids()  # type: ignore

    def get_inputs_for_steps(self, *step_ids: str) -> Dict[str, Dict[str, uuid.UUID]]:
        """Retrieve value ids for the inputs of the specified steps (or all steps, if no argument provided."""

        result = {}
        for step_id in self._structure.step_ids:
            if step_ids and step_id not in step_ids:
                continue
            ids = self.get_current_step_inputs(step_id=step_id)
            result[step_id] = ids
        return result

    def get_outputs_for_steps(self, *step_ids: str) -> Dict[str, Dict[str, uuid.UUID]]:
        """Retrieve value ids for the outputs of the specified steps (or all steps, if no argument provided."""

        result = {}
        for step_id in self._structure.step_ids:
            if step_ids and step_id not in step_ids:
                continue
            ids = self.get_current_step_outputs(step_id=step_id)
            result[step_id] = ids
        return result

    def _notify_pipeline_listeners(self, event: PipelineEvent):

        for listener in self._listeners:
            listener._pipeline_event_occurred(event=event)

    def get_pipeline_details(self) -> PipelineState:

        pipeline_inputs = self._all_values.get_alias("pipeline.inputs")
        pipeline_outputs = self._all_values.get_alias("pipeline.outputs")

        if pipeline_inputs:
            invalid = pipeline_inputs.check_invalid()
            if not invalid:
                status = StepStatus.INPUTS_READY
                step_outputs = self._all_values.get_alias("pipeline.outputs")
                assert step_outputs is not None
                invalid_outputs = step_outputs.check_invalid()
                # TODO: also check that all the pedigrees match up with current inputs
                if not invalid_outputs:
                    status = StepStatus.RESULTS_READY
            else:
                status = StepStatus.INPUTS_INVALID
            _pipeline_inputs = pipeline_inputs.get_all_value_ids()
        else:
            _pipeline_inputs = {}
            invalid = {}
            status = StepStatus.INPUTS_READY

        if pipeline_outputs:
            _pipeline_outputs = pipeline_outputs.get_all_value_ids()
        else:
            _pipeline_outputs = {}

        step_states = {}
        for step_id in self._structure.step_ids:
            d = self.get_step_details(step_id)
            step_states[step_id] = d

        details = PipelineState.construct(
            kiara_id=self._data_registry.kiara_id,
            pipeline_id=self.pipeline_id,
            pipeline_status=status,
            pipeline_inputs=_pipeline_inputs,
            pipeline_outputs=_pipeline_outputs,
            invalid_details=invalid,
            step_states=step_states,
        )

        return details

    def get_step_details(self, step_id: str) -> StepDetails:

        step_input_ids = self.get_current_step_inputs(step_id=step_id)
        step_output_ids = self.get_current_step_outputs(step_id=step_id)
        step_inputs = self._all_values.get_alias(f"steps.{step_id}.inputs")

        assert step_inputs is not None
        invalid = step_inputs.check_invalid()

        processing_stage = self._structure.get_processing_stage(step_id)

        if not invalid:
            status = StepStatus.INPUTS_READY
            step_outputs = self._all_values.get_alias(f"steps.{step_id}.outputs")
            assert step_outputs is not None
            invalid_outputs = step_outputs.check_invalid()
            # TODO: also check that all the pedigrees match up with current inputs
            if not invalid_outputs:
                status = StepStatus.RESULTS_READY
        else:
            status = StepStatus.INPUTS_INVALID

        details = StepDetails.construct(
            kiara_id=self._data_registry.kiara_id,
            pipeline_id=self.pipeline_id,
            step=self._structure.get_step(step_id=step_id),
            step_id=step_id,
            status=status,
            inputs=step_input_ids,
            outputs=step_output_ids,
            invalid_details=invalid,
            processing_stage=processing_stage,
        )
        return details

    def set_pipeline_inputs(
        self,
        inputs: Mapping[str, Any],
        sync_to_step_inputs: bool = True,
        notify_listeners: bool = True,
    ) -> Mapping[str, Mapping[str, Mapping[str, ChangedValue]]]:

        values_to_set: Dict[str, uuid.UUID] = {}

        for k, v in inputs.items():
            if v is SpecialValue.NOT_SET:
                values_to_set[k] = NOT_SET_VALUE_ID
            elif v in [None, SpecialValue.NO_VALUE]:
                values_to_set[k] = NONE_VALUE_ID
            else:
                alias_map = self._all_values.get_alias("pipeline.inputs")
                assert alias_map is not None
                # dbg(alias_map.__dict__)
                schema = alias_map.values_schema.get(k, None)
                if schema is None:
                    raise Exception(
                        f"Can't set pipeline input for input '{k}': no such input field. Available fields: {', '.join(alias_map.values_schema.keys())}"
                    )
                value = self._data_registry.register_data(
                    data=v, schema=schema, pedigree=ORPHAN, reuse_existing=True
                )
                values_to_set[k] = value.value_id

        if not values_to_set:
            return {}

        changed_pipeline_inputs = self._set_values("pipeline.inputs", **values_to_set)

        changed_results = {"__pipeline__": {"inputs": changed_pipeline_inputs}}

        if sync_to_step_inputs:
            changed = self.sync_pipeline_inputs(notify_listeners=False)
            dpath.merge(changed_results, changed)  # type: ignore

        if notify_listeners:
            event = PipelineEvent.create_event(pipeline=self, changed=changed_results)
            if event:
                self._notify_pipeline_listeners(event)

        return changed_results

    def sync_pipeline_inputs(
        self, notify_listeners: bool = True
    ) -> Mapping[str, Mapping[str, Mapping[str, ChangedValue]]]:

        pipeline_inputs = self.get_current_pipeline_inputs()

        values_to_sync: Dict[str, Dict[str, Union[uuid.UUID, None]]] = {}

        for field_name, ref in self._structure.pipeline_input_refs.items():
            for step_input in ref.connected_inputs:
                step_inputs = self.get_current_step_inputs(step_input.step_id)

                if step_inputs[step_input.value_name] != pipeline_inputs[field_name]:
                    values_to_sync.setdefault(step_input.step_id, {})[
                        step_input.value_name
                    ] = pipeline_inputs[field_name]

        results: Dict[str, Mapping[str, Mapping[str, ChangedValue]]] = {}
        for step_id in values_to_sync.keys():
            values = values_to_sync[step_id]
            step_changed = self._set_step_inputs(step_id=step_id, inputs=values)
            dpath.merge(results, step_changed)  # type: ignore

        if notify_listeners:
            event = PipelineEvent.create_event(pipeline=self, changed=results)
            if event:
                self._notify_pipeline_listeners(event)

        return results

    def _set_step_inputs(
        self, step_id: str, inputs: Mapping[str, Union[uuid.UUID, None]]
    ) -> Mapping[str, Mapping[str, Mapping[str, ChangedValue]]]:

        changed_step_inputs = self._set_values(f"steps.{step_id}.inputs", **inputs)
        if not changed_step_inputs:
            return {}

        result: Dict[str, Dict[str, Dict[str, ChangedValue]]] = {
            step_id: {"inputs": changed_step_inputs}
        }

        step_outputs = self._structure.get_step_output_refs(step_id=step_id)
        null_outputs = {k: NOT_SET_VALUE_ID for k in step_outputs.keys()}

        changed_outputs = self.set_step_outputs(
            step_id=step_id, outputs=null_outputs, notify_listeners=False
        )
        # assert step_id in changed_outputs.keys()

        result.update(changed_outputs)  # type: ignore

        return result

    def set_multiple_step_outputs(
        self,
        changed_outputs: Mapping[str, Mapping[str, Union[uuid.UUID, None]]],
        notify_listeners: bool = True,
    ) -> Mapping[str, Mapping[str, Mapping[str, ChangedValue]]]:

        results: Dict[str, Dict[str, Dict[str, ChangedValue]]] = {}
        for step_id, outputs in changed_outputs.items():
            step_results = self.set_step_outputs(
                step_id=step_id, outputs=outputs, notify_listeners=False
            )
            dpath.merge(results, step_results)  # type: ignore

        if notify_listeners:
            event = PipelineEvent.create_event(pipeline=self, changed=results)
            if event:
                self._notify_pipeline_listeners(event)

        return results

    def set_step_outputs(
        self,
        step_id: str,
        outputs: Mapping[str, Union[uuid.UUID, None]],
        notify_listeners: bool = True,
    ) -> Mapping[str, Mapping[str, Mapping[str, ChangedValue]]]:

        # make sure pedigrees match with respective inputs?

        changed_step_outputs = self._set_values(f"steps.{step_id}.outputs", **outputs)
        if not changed_step_outputs:
            return {}

        result: Dict[str, Dict[str, Dict[str, ChangedValue]]] = {
            step_id: {"outputs": changed_step_outputs}
        }

        output_refs = self._structure.get_step_output_refs(step_id=step_id)

        pipeline_outputs: Dict[str, Union[uuid.UUID, None]] = {}

        inputs_to_set: Dict[str, Dict[str, Union[uuid.UUID, None]]] = {}

        for field_name, ref in output_refs.items():
            if ref.pipeline_output:
                assert ref.pipeline_output not in pipeline_outputs.keys()
                pipeline_outputs[ref.pipeline_output] = outputs[field_name]
            for input_ref in ref.connected_inputs:
                inputs_to_set.setdefault(input_ref.step_id, {})[
                    input_ref.value_name
                ] = outputs[field_name]

        for step_id, step_inputs in inputs_to_set.items():
            changed_step_fields = self._set_step_inputs(
                step_id=step_id, inputs=step_inputs
            )
            dpath.merge(result, changed_step_fields)  # type: ignore

        if pipeline_outputs:
            changed_pipeline_outputs = self._set_pipeline_outputs(**pipeline_outputs)
            dpath.merge(  # type: ignore
                result, {"__pipeline__": {"outputs": changed_pipeline_outputs}}
            )

        if notify_listeners:
            event = PipelineEvent.create_event(pipeline=self, changed=result)
            if event:
                self._notify_pipeline_listeners(event)

        return result

    def _set_pipeline_outputs(
        self, **outputs: Union[uuid.UUID, None]
    ) -> Mapping[str, ChangedValue]:

        changed_pipeline_outputs = self._set_values("pipeline.outputs", **outputs)
        return changed_pipeline_outputs

    def _set_values(
        self, alias: str, **values: Union[uuid.UUID, None]
    ) -> Dict[str, ChangedValue]:
        """Set values (value-ids) for the sub-alias-map with the specified alias path."""

        invalid = {}
        for k in values.keys():
            _alias = self._all_values.get_alias(alias)
            assert _alias is not None
            if k not in _alias.values_schema.keys():
                invalid[
                    k
                ] = f"Invalid field '{k}'. Available fields: {', '.join(self.get_current_pipeline_inputs().keys())}"

        if invalid:
            raise InvalidValuesException(invalid_values=invalid)

        alias_map: Union[AliasValueMap, None] = self._all_values.get_alias(alias)
        assert alias_map is not None

        values_to_set: Dict[str, Union[uuid.UUID, None]] = {}
        current: Dict[str, Union[uuid.UUID, None]] = {}
        changed: Dict[str, ChangedValue] = {}

        for field_name, new_value in values.items():

            current_value = self._all_values.get_alias(f"{alias}.{field_name}")
            if current_value is not None:
                current_value_id = current_value.assoc_value
            else:
                current_value_id = None
            current[field_name] = current_value_id

            if current_value_id != new_value:
                values_to_set[field_name] = new_value
                changed[field_name] = ChangedValue(old=current_value_id, new=new_value)

        _alias = self._all_values.get_alias(alias)
        assert _alias is not None
        _alias._set_aliases(**values_to_set)

        return changed

    @property
    def step_ids(self) -> Iterable[str]:
        """Return all ids of the steps of this pipeline."""
        return self._structure.step_ids

    @property
    def execution_graph(self) -> nx.DiGraph:
        return self._structure.execution_graph

    @property
    def data_flow_graph(self) -> nx.DiGraph:
        return self._structure.data_flow_graph

    @property
    def data_flow_graph_simple(self) -> nx.DiGraph:
        return self._structure.data_flow_graph_simple

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

    def create_job_config_for_step(self, step_id: str) -> JobConfig:

        step_inputs: Mapping[str, uuid.UUID] = self.get_current_step_inputs(step_id)
        step_details: StepDetails = self.get_step_details(step_id=step_id)
        step: PipelineStep = self.get_step(step_id=step_id)

        # if the inputs are not valid, ignore this step
        if step_details.status == StepStatus.INPUTS_INVALID:
            invalid_details = step_details.invalid_details
            assert invalid_details is not None
            msg = f"Can't execute step '{step_id}', invalid inputs: {', '.join(invalid_details.keys())}"
            raise InvalidValuesException(msg=msg, invalid_values=invalid_details)

        job_config = JobConfig.create_from_module(
            data_registry=self._data_registry, module=step.module, inputs=step_inputs
        )

        return job_config

    def create_renderable(self, **config: Any) -> RenderableType:

        return PipelineInfo.create_from_pipeline(
            kiara=self._kiara, pipeline=self
        ).create_renderable(**config)


class PipelineInfo(ItemInfo[Pipeline]):

    _kiara_model_id = "info.pipeline"

    @classmethod
    def base_instance_class(cls) -> Type[Pipeline]:
        return Pipeline

    @classmethod
    def create_from_instance(cls, kiara: "Kiara", instance: Pipeline, **kwargs):

        return cls.create_from_pipeline(kiara=kiara, pipeline=instance)

    @classmethod
    def category_name(cls) -> str:
        return "pipeline"

    @classmethod
    def create_from_pipeline(cls, kiara: "Kiara", pipeline: Pipeline):

        doc = DocumentationMetadataModel.create(None)
        authors = AuthorsMetadataModel()
        context = ContextMetadataModel()

        # stages = PipelineStage.from_pipeline_structure(structure=pipeline.structure)

        pipeline_info = PipelineInfo(
            type_name=str(pipeline.pipeline_id),
            documentation=doc,
            authors=authors,
            context=context,
            # pipeline_structure=pipeline.structure,
            pipeline_config=pipeline.structure.pipeline_config,
            pipeline_state=pipeline.get_pipeline_details(),
            # stages=stages
        )
        pipeline_info._kiara = kiara
        return pipeline_info

    # pipeline_structure: PipelineStructure = Field(description="The pipeline structure.")
    pipeline_config: PipelineConfig = Field(
        description="The configuration of the pipeline."
    )
    pipeline_state: PipelineState = Field(description="The current input details.")
    # stages: Mapping[int, PipelineStage] = Field(description="Details about this pipelines stages/execution order.")
    _kiara: "Kiara" = PrivateAttr(default=None)
    _structure: "PipelineStructure" = PrivateAttr(default=None)

    @property
    def pipeline_structure(self):
        return self.pipeline_config.structure

    def create_pipeline_table(self, **config: Any) -> Table:

        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
        table.add_column("property", style="i")
        table.add_column("value")

        self._fill_table(table=table, config=config)
        return table

    def _fill_table(self, table: Table, config: Mapping[str, Any]):

        include_pipeline_inputs = config.get("include_pipeline_inputs", True)
        include_pipeline_outputs = config.get("include_pipeline_outputs", True)
        include_steps = config.get("include_steps", True)

        if include_pipeline_inputs:
            input_values = self._kiara.data_registry.create_valuemap(
                data=self.pipeline_state.pipeline_inputs,
                schema=self.pipeline_structure.pipeline_inputs_schema,
            )

            ordered_fields: Dict[str, List[str]] = {}
            for field_name, ref in self.pipeline_structure.pipeline_input_refs.items():
                for con_input in ref.connected_inputs:
                    details = self.pipeline_structure.get_step_details(
                        step_id=con_input.step_id
                    )
                    stage = details.processing_stage
                    ordered_fields.setdefault(stage, []).append(field_name)

            fields = []
            for stage in sorted(ordered_fields.keys()):

                for f in sorted(ordered_fields[stage]):
                    if f not in fields:
                        fields.append(f)

            inputs = create_value_map_status_renderable(
                input_values,
                render_config={
                    "show_description": False,
                    "show_type": False,
                    "show_default": True,
                    "show_value_ids": True,
                },
                fields=fields,
            )

            table.add_row("pipeline inputs", inputs)
        if include_steps:
            steps = create_pipeline_steps_tree(
                pipeline_structure=self.pipeline_structure,
                pipeline_details=self.pipeline_state,
            )
            table.add_row("steps", steps)

        if include_pipeline_outputs:
            output_values = self._kiara.data_registry.load_values(
                values=self.pipeline_state.pipeline_outputs
            )
            ordered_fields = {}
            for (
                field_name,
                o_ref,
            ) in self.pipeline_structure.pipeline_output_refs.items():
                con_step_id = o_ref.connected_output.step_id
                details = self.pipeline_structure.get_step_details(step_id=con_step_id)
                stage = details.processing_stage
                ordered_fields.setdefault(stage, []).append(field_name)

            fields = []
            for stage in sorted(ordered_fields.keys()):
                for f in sorted(ordered_fields[stage]):
                    fields.append(f)

            t_outputs = create_value_map_status_renderable(
                output_values,
                render_config={
                    "show_description": False,
                    "show_type": True,
                    "show_default": False,
                    "show_required": False,
                    "show_value_ids": True,
                },
                fields=fields,
            )

            table.add_row("pipeline outputs", t_outputs)

    def create_renderable(self, **config: Any) -> RenderableType:

        include_details = config.get("include_details", False)
        include_doc = config.get("include_doc", False)
        include_authors = config.get("include_authors", False)
        include_context = config.get("include_context", False)
        include_structure = config.get("include_structure", False)

        table = self.create_pipeline_table(**config)

        if include_details:
            t_details = create_table_from_model_object(
                self.pipeline_state, render_config=config
            )
            table.add_row("details", t_details)

        if include_doc:
            table.add_row(
                "Documentation",
                Panel(self.documentation.create_renderable(), box=box.SIMPLE),
            )
        if include_authors:
            table.add_row("Author(s)", self.authors.create_renderable(**config))
        if include_context:
            table.add_row("Context", self.context.create_renderable(**config))

        if include_structure:
            table.add_row(
                "Pipeline structure",
                self.pipeline_structure.create_renderable(**config),
            )

        return table
